//
//  SQLClient.m
//  SQLClient
//
//  Created by Martin Rybak on 10/4/13.
//  Copyright (c) 2013 Martin Rybak. All rights reserved.
//

#import "SQLClient.h"
#import "sybfront.h"
#import "sybdb.h"
#import "syberror.h"

int const SQLClientDefaultTimeout = 5;
int const SQLClientDefaultQueryTimeout = 5;
NSString* const SQLClientDefaultCharset = @"UTF-8";
NSString* const SQLClientWorkerQueueName = @"com.martinrybak.sqlclient";
NSString* const SQLClientDelegateError = @"Delegate must be set to an NSObject that implements the SQLClientDelegate protocol";
NSString* const SQLClientRowIgnoreMessage = @"Ignoring unknown row type";

struct COL
{
	char* name;
	char* buffer;
	int type;
	int size;
	int status;
};

@interface SQLClient ()

@property (nonatomic, copy, readwrite) NSString* host;
@property (nonatomic, copy, readwrite) NSString* username;
@property (nonatomic, copy, readwrite) NSString* database;

@end

@implementation SQLClient
{
	LOGINREC* login;
	DBPROCESS* connection;
	char* _password;
}

#pragma mark - NSObject

//Initializes the FreeTDS library and sets callback handlers
- (id)init
{
    if (self = [super init])
    {
        //Initialize the FreeTDS library
        if (dbinit() == FAIL)
			return nil;
		
		//Initialize SQLClient
		self.timeout = SQLClientDefaultTimeout;
		self.charset = SQLClientDefaultCharset;
		self.callbackQueue = [NSOperationQueue currentQueue];
		self.workerQueue = [[NSOperationQueue alloc] init];
		self.workerQueue.name = SQLClientWorkerQueueName;
		
        //Set FreeTDS callback handlers
        dberrhandle(err_handler);
        dbmsghandle(msg_handler);
    }
    return self;
}

//Exits the FreeTDS library
- (void)dealloc
{
    dbexit();
}

#pragma mark - Public

+ (instancetype)sharedInstance
{
    static SQLClient* sharedInstance = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        sharedInstance = [[self alloc] init];
    });
    return sharedInstance;
}

- (void)connect:(NSString*)host
	   username:(NSString*)username
	   password:(NSString*)password
	   database:(NSString*)database
	 completion:(SQLConnectionSuccess)completion
{
	//Save inputs
	self.host = host;
	self.username = username;
	self.database = database;

	/*
	Copy password into a global C string. This is because in connectionSuccess: and connectionFailure:,
	dbloginfree() will attempt to overwrite the password in the login struct with zeroes for security.
	So it must be a string that stays alive until then. Passing in [password UTF8String] does not work because:
		 
	"The returned C string is a pointer to a structure inside the string object, which may have a lifetime
	shorter than the string object and will certainly not have a longer lifetime. Therefore, you should
	copy the C string if it needs to be stored outside of the memory context in which you called this method."
	https://developer.apple.com/library/mac/documentation/Cocoa/Reference/Foundation/Classes/NSString_Class/Reference/NSString.html#//apple_ref/occ/instm/NSString/UTF8String
	 */
	 _password = strdup([password UTF8String]);
	
	//Connect to database on worker queue
	[self.workerQueue addOperationWithBlock:^{
	
		//Set login timeout
		dbsetlogintime(self.timeout);
		
		//Initialize login struct
		if ((login = dblogin()) == FAIL)
			return [self connectionFailure:completion];
		
		//Populate login struct
		DBSETLUSER(login, [self.username UTF8String]);
		DBSETLPWD(login, _password);
		DBSETLHOST(login, [self.host UTF8String]);
		DBSETLCHARSET(login, [self.charset UTF8String]);
		
		//Connect to database server
		if ((connection = dbopen(login, [self.host UTF8String])) == NULL)
			return [self connectionFailure:completion];
		
		//Switch to database
		if (dbuse(connection, [self.database UTF8String]) == FAIL)
			return [self connectionFailure:completion];
	
		//Success!
		[self connectionSuccess:completion];
	}];
}

- (BOOL)connected
{
	return !dbdead(connection);
}

// TODO: how to handle SQL stored procedure output parameters
- (void)executeReader:(NSString*)sql completion:(SQLQueryResults)completion
{
	//Execute query on worker queue
	[self.workerQueue addOperationWithBlock:^{
		
		//Set query timeout
		dbsettime(self.timeout);
		
        //Execute SQL statement
        if ([self executeSQLStatement:sql] == FAIL) {
            return [self executionFailure:nil];
        }
		
		//Create array to contain the tables
		NSMutableArray* output = [[NSMutableArray alloc] init];
		
		struct COL* columns;
		struct COL* pcol;
		int erc;
		
		//Loop through each table
		while ((erc = dbresults(connection)) != NO_MORE_RESULTS)
		{
			int ncols;
			int row_code;
						
			//Create array to contain the rows for this table
			NSMutableArray* table = [[NSMutableArray alloc] init];
			
			//Get number of columns
			ncols = dbnumcols(connection);
			
			//Allocate C-style array of COL structs
			if ((columns = calloc(ncols, sizeof(struct COL))) == NULL)
				return [self executionFailure:completion];
			
			//Bind the column info
			for (pcol = columns; pcol - columns < ncols; pcol++)
			{
				//Get column number
				int c = pcol - columns + 1;
				
                //Assign metada to column
                erc = [self assignMetadaToColumn:pcol forIndex:c];
                if (erc == FAIL)
                    return [self executionFailure:completion];
				
				//printf("%s is type %d with value %s\n", pcol->name, pcol->type, pcol->buffer);
			}
			
			//printf("\n");
			
			//Loop through each row
			while ((row_code = dbnextrow(connection)) != NO_MORE_ROWS)
			{
                //Check row type
                switch (row_code)
                {
                        //Regular row
                    case REG_ROW:
                    {
                        //Create a new dictionary to contain the column names and values
                        NSMutableDictionary* row = [self createRowFromColumns:columns totalOfColumns:ncols];
                        
                        //Add an immutable copy to the table
                        [table addObject:[row copy]];
                        //printf("\n");
                        break;
                    }
                        //Buffer full
                    case BUF_FULL:
                        return [self executionFailure:completion];
                        //Error
                    case FAIL:
                        return [self executionFailure:completion];
                    default:
                        [self message:SQLClientRowIgnoreMessage];
                }
			}
			
			//Clean up
			for (pcol = columns; pcol - columns < ncols; pcol++)
				free(pcol->buffer);
			free(columns);
			
			//Add immutable copy of table to output
			[output addObject:[table copy]];
		}
		
        DBINT rowsAffected = dbcount(connection);
        
        //Success! Send an immutable copy of the results array
		[self executionSuccess:completion results:[output copy] rowsAffected:rowsAffected];
	}];
}

- (void)executeScalar:(NSString*)sql completion:(SQLQueryResults)completion
{
    //Execute query on worker queue
    [self.workerQueue addOperationWithBlock:^{
        
        //Set query timeout
        dbsettime(self.timeout);
        
        //Execute SQL statement
        if ([self executeSQLStatement:sql] == FAIL) {
            return [self executionFailure:nil];
        }
        
        //Create array to contain the tables
        NSMutableArray* output = [[NSMutableArray alloc] init];
        
        struct COL* pcol;
        int erc;
        
        //Loop through each table
        if ((erc = dbresults(connection)) != NO_MORE_RESULTS)
        {
            int row_code;
            
            //Create array to contain the rows for this table
            NSMutableArray* table = [[NSMutableArray alloc] init];
            
            //Allocate C-style array of COL structs
            if ((pcol = calloc(1, sizeof(struct COL))) == NULL)
                return [self executionFailure:completion];
            
            //Assign metada to column
            erc = [self assignMetadaToColumn:pcol forIndex:1];
            if (erc == FAIL)
                return [self executionFailure:completion];
            
            //printf("\n");
            
            //Loop through each row
            if ((row_code = dbnextrow(connection)) != NO_MORE_ROWS)
            {
                //Check row type
                switch (row_code)
                {
                        //Regular row
                    case REG_ROW:
                    {
                        //Create a new dictionary to contain the column names and values
                        NSMutableDictionary* row = [self createRowFromColumns:pcol totalOfColumns:1];
                        
                        //Add an immutable copy to the table
                        [table addObject:[row copy]];
                        //printf("\n");
                        break;
                    }
                        //Buffer full
                    case BUF_FULL:
                        return [self executionFailure:completion];
                        //Error
                    case FAIL:
                        return [self executionFailure:completion];
                    default:
                        [self message:SQLClientRowIgnoreMessage];
                }
            }
            
            //Clean up
            free(pcol->buffer);
            
            //Add immutable copy of table to output
            [output addObject:[table copy]];
        }
        
        DBINT rowsAffected = dbcount(connection);
        
        if (rowsAffected > 1)
            rowsAffected = 1;
        
        //Success! Send an immutable copy of the results array
        [self executionSuccess:completion results:[output copy] rowsAffected:rowsAffected];
    }];
}

- (void)executeNonQuery:(NSString*)sql completion:(SQLQueryResults)completion
{
    //Execute query on worker queue
    [self.workerQueue addOperationWithBlock:^{
        
        //Execute SQL statement
        if ([self executeSQLStatement:sql] == FAIL)
            return [self executionFailure:nil];
        
        DBINT rowsAffected = dbcount(connection);
        
        //Success! Send affected rows of query
        [self executionSuccess:completion results:nil rowsAffected:rowsAffected];
    }];
}

- (void)disconnect
{
    dbclose(connection);
}

#pragma mark - Private

//Execute SQL statement
- (int)executeSQLStatement:(NSString*)sql
{
    //Prepare SQL statement
    if (dbcmd(connection, [sql UTF8String]) == FAIL)
        return FAIL;
    
    //Execute SQL statement
    if (dbsqlexec(connection) == FAIL)
        return FAIL;
    
    return SUCCEED;
}

//Retrieves column metadata from database
- (int)assignMetadaToColumn:(struct COL *)column forIndex:(int)index
{
    int erc;
    
    //Get column metadata
    column->name = dbcolname(connection, index);
    column->type = dbcoltype(connection, index);
    
    //For IMAGE data, we need to multiply by 2, because dbbind() will convert each byte to a hexadecimal pair.
    //http://www.freetds.org/userguide/samplecode.htm#SAMPLECODE.RESULTS
    if(column->type == SYBIMAGE){
        column->size = dbcollen(connection, index) * 2;
    }else{
        column->size = dbcollen(connection, index);
    }
    
    //If the column is [VAR]CHAR or TEXT, we want the column's defined size, otherwise we want
    //its maximum size when represented as a string, which FreeTDS's dbwillconvert()
    //returns (for fixed-length datatypes). We also do not need to convert IMAGE data type
    if (column->type != SYBCHAR && column->type != SYBTEXT && column->type != SYBIMAGE)
        column->size = dbwillconvert(column->type, SYBCHAR);
    
    //Allocate memory in the current pcol struct for a buffer
    if ((column->buffer = calloc(1, column->size + 1)) == NULL)
        return FAIL;
    
    //Bind column name
    erc = dbbind(connection, index, NTBSTRINGBIND, column->size + 1, (BYTE*)column->buffer);
    if (erc == FAIL)
        return FAIL;
    
    //Bind column status
    erc = dbnullbind(connection, index, &column->status);
    if (erc == FAIL)
        return FAIL;
    
    return SUCCEED;
}

//Generate a row table from columns data
- (NSMutableDictionary *)createRowFromColumns:(struct COL*)columns totalOfColumns:(int)total
{
    struct COL* pcol;
    
    //Create a new dictionary to contain the column names and values
    NSMutableDictionary* row = [[NSMutableDictionary alloc] initWithCapacity:total];
    
    //Loop through each column and create an entry where dictionary[columnName] = columnValue
    for (pcol = columns; pcol - columns < total; pcol++)
    {
        NSString* column = [NSString stringWithUTF8String:pcol->name];
        
        id value;
        if (pcol->status == -1) { //null value
            value = [NSNull null];
            
            //Converting hexadecimal buffer into NSImage
        }else if (pcol->type == SYBIMAGE){
            NSString *hexString = [[NSString stringWithUTF8String:pcol->buffer] stringByReplacingOccurrencesOfString:@" " withString:@""];
            NSMutableData *hexData = [[NSMutableData alloc] init];
            
            //Converting hex string to NSData
            unsigned char whole_byte;
            char byte_chars[3] = {'\0','\0','\0'};
            int i;
            for (i=0; i < [hexString length]/2; i++) {
                byte_chars[0] = [hexString characterAtIndex:i*2];
                byte_chars[1] = [hexString characterAtIndex:i*2+1];
                whole_byte = strtol(byte_chars, NULL, 16);
                [hexData appendBytes:&whole_byte length:1];
            }
            value = [[UIImage alloc] initWithData:hexData];
        }else {
            value = [NSString stringWithUTF8String:pcol->buffer];
        }
        
        row[column] = value;
    }
    
    return row;
}

//Invokes connection completion handler on callback queue with success = NO
- (void)connectionFailure:(SQLConnectionSuccess)completion
{
    [self.callbackQueue addOperationWithBlock:^{
        if (completion)
            completion(NO);
    }];
    
    //Cleanup
    dbloginfree(login);
	free(_password);
}

//Invokes connection completion handler on callback queue with success = [self connected]
- (void)connectionSuccess:(SQLConnectionSuccess)completion
{
    [self.callbackQueue addOperationWithBlock:^{
        if (completion)
            completion([self connected]);
    }];
    
    //Cleanup
    dbloginfree(login);
	free(_password);
}

//Invokes execution completion handler on callback queue with results = nil
- (void)executionFailure:(SQLQueryResults)completion
{
    [self.callbackQueue addOperationWithBlock:^{
        if (completion)
            completion(nil, -1);
    }];
    
    //Clean up
    dbfreebuf(connection);
}

//Invokes execution completion handler on callback queue with results array
- (void)executionSuccess:(SQLQueryResults)completion results:(NSArray*)results rowsAffected:(int)rowsAffected
{
    [self.callbackQueue addOperationWithBlock:^{
        if (completion)
            completion(results, rowsAffected);
    }];
    
    //Clean up
    dbfreebuf(connection);
}

//Handles message callback from FreeTDS library.
int msg_handler(DBPROCESS* dbproc, DBINT msgno, int msgstate, int severity, char* msgtext, char* srvname, char* procname, int line)
{
	//Can't call self from a C function, so need to access singleton
	SQLClient* self = [SQLClient sharedInstance];
	[self message:[NSString stringWithUTF8String:msgtext]];
	return 0;
}

//Handles error callback from FreeTDS library.
int err_handler(DBPROCESS* dbproc, int severity, int dberr, int oserr, char* dberrstr, char* oserrstr)
{
	//Can't call self from a C function, so need to access singleton
	SQLClient* self = [SQLClient sharedInstance];
	[self error:[NSString stringWithUTF8String:dberrstr] code:dberr severity:severity];
	return INT_CANCEL;
}

//Forwards a message to the delegate on the callback queue if it implements
- (void)message:(NSString*)message
{
	//Invoke delegate on calling queue
	[self.callbackQueue addOperationWithBlock:^{
		if ([self.delegate respondsToSelector:@selector(message:)])
			[self.delegate message:message];
	}];
}

//Forwards an error message to the delegate on the callback queue.
- (void)error:(NSString*)error code:(int)code severity:(int)severity
{
	if (!self.delegate || ![self.delegate conformsToProtocol:@protocol(SQLClientDelegate)])
		[NSException raise:SQLClientDelegateError format:nil];
	
	//Invoke delegate on callback queue
	[self.callbackQueue addOperationWithBlock:^{
		[self.delegate error:error code:code severity:severity];
	}];
}

@end
