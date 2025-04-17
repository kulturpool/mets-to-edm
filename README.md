# mets_to_edm

Library to convert Mets/Mods data to EDM

## Usage

### From Python
```
from mets_to_edm import MetsToEdmMapper

# Create an edmlib record object from a mets/mods record already parsed in lxml 
edmlib_record = MetsToEdmMapper.process_record(your_mets_lxml_tree)

# To get the xml serialized version
edm_xml_record = edmlib_record.serialize()
```

To change specific mapping behaviour create a subclass of MetsToEdmMapper and overwrite the specific functions

### From CLI
```
python -m mets_to_edm example.xml
```