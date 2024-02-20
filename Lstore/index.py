from btree import BTree

class Index:
    """
    Creates an index object for efficient searching within a table.

    Args:
        table (Table): The table object to create indices for.
    """

    def __init__(self, table):
        """
        Initializes the index object with empty indices for each column.

        Args:
            table (Table): The table object to create indices for.
        """
        self.table = table
        self.indices = [None] * table.num_columns

        # Create index for the key column by default.
        self.create_index(table.key_column)

    def locate(self, column, value):
        """
        Returns the record IDs (RIDs) of all records with the given value in the specified column.

        Args:
            column (int): The number of the column to search (zero-based indexing).
            value: The value to search for.

        Returns:
            list: A list of RIDs for matching records, or an empty list if no match is found.

        Raises:
            ValueError: If the specified column does not have an index.
        """

        if self.indices[column] is None:
            raise ValueError(f"No index exists for column {column}.")
        return self.indices[column].search(value)

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in the specified column between `begin` and `end`.

        Args:
            begin: The lower bound of the range (inclusive).
            end: The upper bound of the range (exclusive).
            column (int): The number of the column to search (zero-based indexing).

        Returns:
            list: A list of RIDs for matching records, or an empty list if no match is found.

        Raises:
            ValueError: If the specified column does not have an index.
        """

        if self.indices[column] is None:
            raise ValueError(f"No index exists for column {column}.")
        return self.indices[column].search_range(begin, end)

    def create_index(self, column_number):
        """
        Creates an index on the specified column using a B-Tree.

        Args:
            column_number (int): The number of the column to index (zero-based indexing).

        Raises:
            ValueError: If an index already exists for the specified column.
        """

        if self.indices[column_number] is not None:
            raise ValueError(f"Index already exists for column {column_number}.")

        column_data = self.table.get_column_data(column_number)
        self.indices[column_number] = BTree(data=[(value, rid) for rid, value in enumerate(column_data)])

    def drop_index(self, column_number):
        """
        Drops the index on the specified column.

        Args:
            column_number (int): The number of the column to drop the index for (zero-based indexing).

        Raises:
            ValueError: If the specified column does not have an index.
        """

        if self.indices[column_number] is None:
            raise ValueError(f"No index exists for column {column_number}.")
        self.indices[column_number] = None

# Example usage:
table = MyTable()  # Replace with your actual table object
index = Index(table)
rids = index.locate(2, 100)  # Find records with value 100 in column 2
# ...
