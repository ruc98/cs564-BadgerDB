# Remove existing dat files
rm *.dat
# Run parser on all json files
python modified_skeleton_parser.py ebay_data/items-*.json
# Sort files and remove repeats
sort -u ITEMS.dat -o ITEMS.dat
sort -u CATEGORIES.dat -o CATEGORIES.dat
sort -u BIDS.dat -o BIDS.dat
sort -u USERS.dat -o USERS.dat
