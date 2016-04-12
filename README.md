SQLClient
=========

Native Microsoft SQL Server client for OSX. An Objective-C wrapper around the open-source FreeTDS library.

##Sample Usage

<pre>
&#35;import "SQLClient.h"

SQLClient* client = [SQLClient sharedInstance];
client.delegate = self;
[client connect:@"server:port" username:@"user" password:@"pass" database:@"db" completion:^(BOOL success) {
    if (success)
    {
      [client execute:@"SELECT * FROM Users" completion:^(NSArray* results) {
        for (NSArray* table in results)
          for (NSDictionary* row in table)
            for (NSString* column in row)
              NSLog(@"%@=%@", column, row[column]);
        [client disconnect];
      }];
    }
}];

//Required
- (void)error:(NSString*)error code:(int)code severity:(int)severity
{
  NSLog(@"Error #%d: %@ (Severity %d)", code, error, severity);
}
</pre>

##Demo Project
Open the Xcode project inside the **SQLClient** folder.


##Installation

###Manual

1. Drag and drop the contents of the **SQLClient/SQLClient/SQLClient** folder into your Xcode project.
2. Select **Copy items into destination group's folder (if needed)**.
3. Go to Project > Build Phases > Link Binary With Libraries.
3. Click + and add **libiconv.dylib**.

##Documentation

<a href="http://htmlpreview.github.io/?https://raw.github.com/martinrybak/SQLClient/master/SQLClient/SQLClientDocs/html/index.html">SQLClient Class Reference</a>

##Credits

FreeTDS:
http://www.freetds.org

FreeTDS example code in C:
http://freetds.schemamania.org/userguide/samplecode.htm
