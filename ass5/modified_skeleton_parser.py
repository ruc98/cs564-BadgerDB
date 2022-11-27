
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)


'''
String escape function
'''
def escape(val):
    # Surround string with quotes and escape every double quote instance
    return '\"' + sub(r'\"','\"\"',val) + '\"'
"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file

        for item in items:
            """
            TODO: traverse the items dictionary to extract information from the
            given `json_file' and generate the necessary .dat files to generate
            the SQL tables based on your relation design
            """
            # Get ITEMS information
            row = ""
            # Get ItemID
            if item["ItemID"] == None:
                row += "NULL"
            else:
                row += item["ItemID"]
            # Get Name
            if "Name" not in item or item["Name"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + escape(item["Name"])
            # Get Currently
            if "Currently" not in item or item["Currently"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + transformDollar(item["Currently"])
            # Get Buy price
            if "Buy_Price" not in item or item["Buy_Price"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + transformDollar(item["Buy_Price"])
            # Get First Bid
            if "First_Bid" not in item or item["First_Bid"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + transformDollar(item["First_Bid"])
            # Get Number of Bids
            if "Number_of_Bids" not in item or item["Number_of_Bids"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + item["Number_of_Bids"]
            # Get Started
            if "Started" not in item or item["Started"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + transformDttm(item["Started"])
            # Get Ends
            if "Ends" not in item or item["Ends"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + transformDttm(item["Ends"])
            # Get Description
            if "Description" not in item or item["Description"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + escape(item["Description"])
            # Get SellerID
            if item["Seller"]["UserID"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + escape(item["Seller"]["UserID"])
            with open("ITEMS.dat","a+") as f:
                f.write(row+"\n")

            # Get CATEGORIES information
            # Set to eliminate duplicates
            categs = set()
            for category in item["Category"]:
                # Get Category
                row = escape(category)
                # Get ItemID
                if item["ItemID"] == None:
                    row += "|"+"NULL"
                else:
                    row += "|"+item["ItemID"]
                # Add row to set
                categs.add(row)
            with open("CATEGORIES.dat","a+") as f:
                for row in categs:
                    f.write(row+"\n")

            # Get BIDS information
            # Set to eliminate duplicates
            bids = set()
            if item["Bids"] != None:
                for i in range(len(item["Bids"])):
                    bid = item["Bids"][i]["Bid"]
                    row = ""
                    # Get ItemID
                    if item["ItemID"] == None:
                        row += "NULL"
                    else:
                        row += item["ItemID"]
                    # Get BidderID
                    if bid["Bidder"]["UserID"] == None:
                        row += "|"+"NULL"
                    else:
                        row += "|" + escape(bid["Bidder"]["UserID"])
                    # Get Time
                    if bid["Time"] == None:
                        row += "|" + "NULL"
                    else:
                        row += "|" + transformDttm(bid["Time"])
                    # Get Amount
                    if bid["Amount"] == None:
                        row += "|" + "NULL"
                    else:
                        row += "|" + transformDollar(bid["Amount"])
                    # Add row to set
                    bids.add(row)
            # Write row to file
            with open("BIDS.dat","a+") as f:
                for row in bids:
                    f.write(row+"\n")

            # Get USERS information
            # Set to eliminate duplicates
            users = set()
            # Get all bidding users
            if item["Bids"] !=None:
                for i in range(len(item["Bids"])):
                    bidder = item["Bids"][i]["Bid"]["Bidder"]
                    row = ""
                    # Get UserID
                    if bidder["UserID"] == None:
                        row += "NULL"
                    else:
                        row += escape(bidder['UserID'])
                    # Get Location
                    if "Location" not in bidder or bidder["Location"] == None:
                        row += "|" + "NULL"
                    else:
                        row += "|" + escape(bidder["Location"])
                    # Get Rating
                    if bidder["Rating"] == None:
                        row += "|" + "NULL"
                    else:
                        row += "|" + bidder["Rating"]
                    # Get Country
                    if "Country" not in bidder or bidder["Country"] == None:
                        row += "|" + "NULL"
                    else:
                        row += "|" + escape(bidder["Country"])
                    # Add to set
                    users.add(row)

            # Get selling users info
            row = ""
            # Get UserID
            if item["Seller"]["UserID"] == None:
                row = "NULL"
            else:
                row = escape(item["Seller"]["UserID"])
            # Get Location
            if item["Location"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + escape(item["Location"])
            # Get Rating
            if item["Seller"]["Rating"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + item["Seller"]["Rating"]
            # Get Country
            if item["Country"] == None:
                row += "|" + "NULL"
            else:
                row += "|" + escape(item["Country"])
            # Add to set
            users.add(row)

            # Write row to file
            with open("USERS.dat","a+") as f:
                for row in users:
                    f.write(row+"\n")

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print "Success parsing " + f

if __name__ == '__main__':
    main(sys.argv)
