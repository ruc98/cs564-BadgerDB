SELECT COUNT(*) FROM USERS WHERE UserID in (SELECT SellerID FROM ITEMS) AND UserID in (SELECT BidderID FROM BIDS);