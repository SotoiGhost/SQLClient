//
//  ViewController.m
//  SQLClientMac
//
//  Created by Israel Soto on 3/5/16.
//  Copyright © 2016 Israel Soto. All rights reserved.
//

#import "ViewController.h"

@interface ViewController ()

@property (unsafe_unretained) IBOutlet NSTextView *resultTextView;
@property (weak) IBOutlet NSProgressIndicator *spinner;

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    
    // Do any additional setup after loading the view.
    self.spinner.displayedWhenStopped = NO;
    [self.spinner startAnimation:nil];
    
    SQLClient* client = [SQLClient sharedInstance];
    client.delegate = self;
    [client connect:@"172.20.10.5\\IGHOST10SQL" username:@"ISSC411" password:@"ISSC411" database:@"ISSC411Test" completion:^(BOOL success) {
        if (success)
        {
            [client executeScalar:@"SELECT COUNT(*) AS Result FROM Alumnos" completion:^(NSArray* results, int rowsAffected) {
                [self process:results];
                [self.spinner stopAnimation:nil];
                self.resultTextView.string = [self.resultTextView.string stringByAppendingFormat:@"\nTotal of rows affected: %i", rowsAffected];
                [client disconnect];
            }];
        }
        else
            [self.spinner stopAnimation:nil];
    }];
}

- (void)setRepresentedObject:(id)representedObject {
    [super setRepresentedObject:representedObject];

    // Update the view, if already loaded.
}

- (void)process:(NSArray*)data
{
    NSMutableString* results = [[NSMutableString alloc] init];
    for (NSArray* table in data)
        for (NSDictionary* row in table)
            for (NSString* column in row)
                [results appendFormat:@"\n%@=%@", column, row[column]];
    
    self.resultTextView.string = results;
}

#pragma mark - SQLClientDelegate

//Required
- (void)error:(NSString*)error code:(int)code severity:(int)severity
{
    NSLog(@"Error #%d: %@ (Severity %d)", code, error, severity);
    
    NSAlert *alert = [[NSAlert alloc] init];
    [alert addButtonWithTitle:@"OK"];
    [alert setMessageText:@"Hubo un error al ejecutar la consulta."];
    [alert setInformativeText:error.description];
    [alert setAlertStyle:NSCriticalAlertStyle];
    [alert runModal];
}

//Optional
- (void)message:(NSString*)message
{
    NSLog(@"Message: %@", message);
}

@end
