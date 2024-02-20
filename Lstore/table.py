# from lstore.index import Index
from time import time
from lstore.page import Page, PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        
    def setRID(self):
        self.rid = self.columns[self.key]

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_column: int      #Index of table key in columns
    """
    def __init__(self, name, num_columns, key_column):
    
        # Table Attributes
        self.name = name
        self.key_column = key_column
        self.num_columns = num_columns
        
        # Page Range List
        self.page_range_list = [[PageRange() for _ in range(self.num_columns)]]
        self.page_range_index = 0
        
        # Indexing
        self.page_directory = {}
#        self.index = Index(self)


    def write(self, record):
        
        # Placeholder Values for the Address
        page_range_index = 0
        page_index = 0
        data_index = 0
        
        # Check if Record Already Exists in the Table
        if record[self.key_column] in self.page_directory:
            print(f"Error: Record with RID {RID} is already in the Table.")
            return
        
        # For each column of data in the record
        for i, data in enumerate(record):
            
            # Get the Current Page
            page_range = self.page_range_list[self.page_range_index][i]
            page = page_range.get_current_page()
            
            # Start a New Page if the Current Page has Reached Maximum Capacity
            if not page.has_capacity():
            
                # Check if the Page Range Can Accomodate another Page
                if page_range.has_capacity():
                    page_range.add_page()
                    
                # Otherwise Start a New Page Range
                else:
                    self.page_range_list.append([PageRange() for _ in range(self.num_columns)])
                    self.page_range_index += 1
                    page_range = self.page_range_list[self.page_range_index][i]
                    
                # Get the New Current Page
                page = page_range.get_current_page()
                
            # Write the Column Data to the Page
            page.write(data)
            
            # Get the Address of the Written Data
            page_range_index = self.page_range_index
            page_index = page_range.get_current_index()
            data_index = page.get_current_index()
            
        # Store the Record Location in the Page Directory
        RID = record[self.key_column]
        self.page_directory[RID] = (page_range_index, page_index, data_index)
      
      
    def delete(self, RID):
        
        # Get the Address of the Record with the Given Primary Key
        if RID not in self.page_directory:
            print(f"Error: Record with RID {RID} was not found in the Table.")
        address = self.page_directory[RID]
        
        # Unpack the Address
        page_range_index, page_index, data_index = address
        
        # Delete the Data in All Columns
        for i in range(self.num_columns):
            page_range = self.page_range_list[page_range_index][i]
            page = page_range.get_page(page_index)
            page.delete(data_index)
            
        # Remove the Address from the Page Directory
        del self.page_directory[RID]
        
        
    def update(self, RID, record):
        
        # Get the Address of the Record with the Given Primary Key
        if RID not in self.page_directory:
            print(f"Error: Record with RID {RID} was not found in the Table.")
        address = self.page_directory[RID]
        
        # Unpack the Address
        page_range_index, page_index, data_index = address
        
        # Update the Data in All Columns
        for i, value in enumerate(record):
            if value is None: continue
            page_range = self.page_range_list[page_range_index][i]
            page = page_range.get_page(page_index)
            page.update(data_index, value)
        
    
    def select(self, search_key, search_column, select_columns):
    
        # Store Record Information
        addresses = []
        records = []
    
        # Find all instances of the search key in the search column
        for i, page_range in enumerate(self.page_range_list):
            for j, page in enumerate(page_range[search_column].pages):
                for k in range(page.num_records):
                    if page.read(k) == search_key:
                        addresses.append((i, j, k))
                        
        # Return all records based on select columns at the addresses
        for address in addresses:
        
            # Create a list to store column values
            record = Record(self.find_value(0, address), 0, [])
            
            # Add Values from Selected Columns to the Record
            for i in range(self.num_columns):
                if select_columns[i]:
                    value = self.find_value(i, address)
                    record.columns.append(value)
                    
            # Add the Record to the Records List
            records.append(record)

        # Return the Records List
        return records
                

    def find_value(self, column_index, location):

        # Unpack the Address
        page_range_index, page_index, data_index = location

        # Locate the Data
        page_range = self.page_range_list[page_range_index][column_index]
        page = page_range.get_page(page_index)
        data = page.read(data_index)

        return data

    
    def sum(self, start_index, end_index, column):
    
        # Check if the Indexes Exist in the Table
        if start_index not in self.page_directory:
            print(f"Error: Record with RID {start_index} was not found in the Table.")
            return
        if end_index not in self.page_directory:
            print(f"Error: Record with RID {end_index} was not found in the Table.")
            return
            
        # Retrieve the Addresses of the Indexes
        start_address = self.page_directory[start_index]
        end_address = self.page_directory[end_index]
        
        # Unpack the Address Tuples
        start_page_range_index, start_page_index, start_data_index = start_address
        end_page_range_index, end_page_index, end_data_index = end_address
        
        # Edge Case: The Indexes are on the Same Page Range
        if start_page_range_index == end_page_range_index:
            return self.page_range_list[start_page_range_index][column].sum(start_page_index, start_data_index, end_page_index, end_data_index)
        
        # Sum up the Head and Tail Page Ranges
        s = 0
        s += self.page_range_list[start_page_range_index][column].sum(start_page_index, start_data_index, None, None)
        s += self.page_range_list[end_page_range_index][column].sum(None, None, end_page_index, end_data_index)
        
        # Increment and Decrement the Page Range Indexes
        start_page_range_index += 1
        end_page_range_index -= 1
        
        # Sum up the Middle Page Ranges
        for i in range(start_page_range_index, end_page_range_index): s += self.page_range_list[i][column].sum()
        
        # Return the Sum
        return s


#    def update_value(self, column_index, location, value):
#
#        # Unpack the Address
#        page_range_index, page_index, data_index = location
#
#        # Locate the Data
#        page_range = self.page_range_list[page_range_index][column+index]
#        page = page_range.get_page(page_index)
#        page.update(data_index, value)
#
#
#    def find_record(self, rid):
#
#        # Retrieve the Location using the Record ID
#        location = self.page_directory[rid]
#
#        # Add the Record Values at the given location index
#        record = []
#        for i in range(self.num_columns):
#            record.append(self.find_value(i, location))
#
#        # Return the Record
#        return record


    def __merge(self):
        print("merge is happening")
        pass
