from lstore.config import *

class Page():

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        if self.num_records < RECORDS_PER_PAGE:
            return True
            
    def get_current_index(self):
        return self.num_records - 1

    def write(self, value):
        new_array_location = self.num_records * RECORD_SIZE
        self.data[new_array_location : new_array_location + RECORD_SIZE] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1
        
    def read(self, index):
        location = index * RECORD_SIZE
        return int.from_bytes(self.data[location : location + RECORD_SIZE], 'big')
        
    def update(self, index, value):
        location = index * RECORD_SIZE
        self.data[location : location + RECORD_SIZE] = int(value).to_bytes(8, byteorder='big')
        
    def delete(self, index):
        location = index * RECORD_SIZE
        del self.data[location : location + RECORD_SIZE]
        
    def sum(self, start = None, end = None):
    
        # Initialization
        if start is None: start = 0
        if end is None: end = self.get_current_index()
        
        s = 0
        for i in range(start, end): s += self.read(i)
        return s
        

class PageRange():
    
    def __init__(self):
        self.pages = [Page()]
        self.num_pages = 1
        
    def add_page(self):
        self.pages.append(Page())
        self.num_pages += 1
        
    def get_page(self, index):
        return self.pages[index]
        
    def get_current_page(self):
        return self.pages[self.num_pages - 1]
        
    def get_current_index(self):
        return self.num_pages - 1
        
    def has_capacity(self):
        return self.num_pages < PAGES_PER_RANGE
        
    def sum(self, start = None, page_start = None, end = None, page_end = None):
    
        # Initialization
        if start is None: start = 0
        if page_start is None: page_start = 0
        if end is None: end = self.get_current_index()
        if page_end is None: page_end = self.get_current_page().get_current_index()
        
        # Edge Case: Start and End Page are the Same
        if start == end: return self.pages[start].sum(page_start, page_end)
        
        # Sum up the head and tail pages
        s = self.pages[start].sum(page_start, None) + self.pages[end].sum(None, page_end)
        
        # Increment and Decrement the Page Indexes
        start += 1
        end -= 1
        
        # Sum up the Middle Pages
        for i in range(start, end): s += page[i].sum()
        
        # Return the Sum
        return s
