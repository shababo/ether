from typing import Any, Dict, List, Optional, Union
from pyairtable import Api, Base, Table
from pyairtable.formulas import match
import pandas as pd

class AirtableClient:
    """Base client for interacting with Airtable tables.
    
    This class provides core functionality for reading and writing data to Airtable.
    It is designed to be inherited by specific data services that add domain-specific logic.
    
    Args:
        api_key: Airtable API key or personal access token
        base_id: ID of the Airtable base to connect to
        table_name: Name or ID of the table to operate on
    """
    def __init__(
            self, 
            api_key: str = None,
            bases: list[str] = None, # by base name, not id
        ):
        """Initialize the Airtable client.
        
        Creates API and Table instances for interacting with Airtable.
        """
        bases = bases or []
        self._api = None
        self._base_ids = {}
        self._base = None

        if api_key is not None:
            self._api = Api(api_key)

            # make a dict from base names to base ids
            _bases = self._api.bases()
            
            for base in _bases:
                if len(bases) > 0 and base.name in bases:
                    self._base_ids[base.name] = base.id
                elif len(bases) == 0:
                    self._base_ids[base.name] = base.id
                else:
                    # raise ValueError(f"Base {base.name} not found in {bases}")
                    pass

            
            # if we only have one base, just set it
            if len(bases) == 1:
                self.set_base(bases[0])

    
    def set_base(self, base_name: str):
        """Set the base to use for the current session.
        
        Args:
            base_name: The name of the base to use (not the id)
        """
        if base_name in self._base_ids:
            self._base = self._api.base(self._base_ids[base_name])
        else:
            self._base = None
        

    def get_table(self, table_name: str = None, base_name: str = None, **kwargs) -> Table:
        """Get the pyairtable Table instance for direct access to the API.
        
        Returns:
            Table: The pyairtable Table instance for advanced operations
        """
        if base_name:
            self.set_base(base_name)
            
        return self._base.table(table_name, **kwargs)

    def get_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single record by its ID.
        
        Args:
            record_id: The Airtable record ID
            
        Returns:
            Dict containing the record data if found, None if not found
        """
        try:
            return self.table.get(record_id)
        except:  # If record not found or other error
            return None

    def get_records(
        self, 
        formula: Optional[str] = None,
        fields: Optional[List[str]] = None,
        max_records: Optional[int] = None,
        sort: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve multiple records with optional filtering and sorting.
        
        Args:
            formula: Airtable formula for filtering records
            fields: List of field names to return (returns all if None)
            max_records: Maximum number of records to return
            sort: List of dicts with 'field' and 'direction' keys for sorting
            
        Returns:
            List of record dictionaries matching the query parameters
        """
        return self.table.all(
            formula=formula,
            fields=fields,
            max_records=max_records,
            sort=sort
        )

    def find_records(
        self, 
        field_matches: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Find records matching field values using exact matching.
        
        Args:
            field_matches: Dict of field names and values to match against
            
        Returns:
            List of matching record dictionaries
        """
        formula = match(field_matches)
        return self.get_records(formula=formula)

    def create_record(
        self, 
        fields: Dict[str, Any],
        typecast: bool = False
    ) -> Dict[str, Any]:
        """Create a new record in the table.
        
        Args:
            fields: Dict of field names and values for the new record
            typecast: Whether to let Airtable attempt to convert string values
            
        Returns:
            Dict containing the created record data
        """
        return self.table.create(fields, typecast=typecast)

    def create_records(
        self, 
        records: List[Dict[str, Any]],
        typecast: bool = False
    ) -> List[Dict[str, Any]]:
        """Create multiple records in batch.
        
        Args:
            records: List of field dicts for the new records
            typecast: Whether to let Airtable attempt to convert string values
            
        Returns:
            List of created record dictionaries
        """
        return self.table.batch_create(records, typecast=typecast)

    def update_record(
        self,
        record_id: str,
        fields: Dict[str, Any],
        typecast: bool = False
    ) -> Dict[str, Any]:
        """Update an existing record.
        
        Args:
            record_id: The Airtable record ID to update
            fields: Dict of field names and values to update
            typecast: Whether to let Airtable attempt to convert string values
            
        Returns:
            Dict containing the updated record data
        """
        return self.table.update(record_id, fields, typecast=typecast)

    def update_records(
        self,
        records: List[Dict[str, Any]],
        typecast: bool = False
    ) -> List[Dict[str, Any]]:
        """Update multiple records in batch.
        
        Args:
            records: List of dicts with 'id' and 'fields' keys
            typecast: Whether to let Airtable attempt to convert string values
            
        Returns:
            List of updated record dictionaries
        """
        return self.table.batch_update(records, typecast=typecast)

    def delete_record(self, record_id: str) -> bool:
        """Delete a single record.
        
        Args:
            record_id: The Airtable record ID to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            result = self.table.delete(record_id)
            return bool(result and result.get('deleted'))
        except:
            return False

    def delete_records(self, record_ids: List[str]) -> bool:
        """Delete multiple records in batch.
        
        Args:
            record_ids: List of Airtable record IDs to delete
            
        Returns:
            True if all deletions were successful
        """
        try:
            results = self.table.batch_delete(record_ids)
            return all(r.get('deleted', False) for r in results)
        except:
            return False

    def create_table(
        self,
        name: str,
        fields: List[Dict[str, Any]],
        description: Optional[str] = None
    ) -> Table:
        """Create a new table in the base.
        
        Args:
            name: The unique table name
            fields: List of field configurations following Airtable's field model
            description: Optional table description
            
        Returns:
            Table: The newly created table instance
        """
        return self.base.create_table(name, fields, description=description)

    def get_base_schema(self) -> Dict[str, Any]:
        """Get the schema for the entire base.
        
        Returns:
            Dict containing the base schema including tables, fields, and views
        """
        return self.base.schema()

    def get_table_schema(self) -> Dict[str, Any]:
        """Get the schema for the current table.
        
        Returns:
            Dict containing the table schema including fields and views
        """
        return self.table.schema()

    def batch_upsert(
        self,
        records: List[Dict[str, Any]],
        key_fields: List[str],
        typecast: bool = False
    ) -> Dict[str, Any]:
        """Create or update records based on matching field values.
        
        Args:
            records: List of records to upsert
            key_fields: Field names to use for matching existing records
            typecast: Whether to let Airtable attempt to convert string values
            
        Returns:
            Dict containing created/updated record information
        """
        return self.table.batch_upsert(
            records=records,
            key_fields=key_fields,
            typecast=typecast
        )
    
    @classmethod
    def clean_table(cls, table: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row_i, row in enumerate(table):
            fields = row.pop("fields")
            if fields:
                for field, value in fields.items():
                    row[field] = value
            table[row_i] = row

        return table
    
    @classmethod
    def table_to_df(cls, table: list[dict[str, Any]]) -> pd.DataFrame:
        table = cls.clean_table(table)

        return pd.DataFrame(table)
        
        
