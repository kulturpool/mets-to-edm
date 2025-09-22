# mets_to_edm

`mets_to_edm` is a Python library for converting METS/MODS XML records into Europeana Data Model (EDM) records.
It can be used both as a library and from the command line and has a basic Mapping that should work well for most cases.
But the library also provides a flexible mapping layer to override certain parts.

## Features

- Converts METS/MODS XML to EDM using [edmlib](https://github.com/kulturpool/EDMLib)
- Easily extensible: override mapping methods to customize output
- CLI and Python API

## Installation

Install via Poetry (recommended):

```sh
poetry install
```

Or with pip (if you have all dependencies):

```sh
pip install .
```

## Usage

### As a Python Library

```python
from mets_to_edm import MetsToEdmMapper
from lxml import etree

# Parse your METS/MODS XML file
xml_tree = etree.parse("example.xml")

# Convert to an EDM record
edmlib_record = MetsToEdmMapper.process_record(xml_tree)

# Serialize to EDM XML
edm_xml = edmlib_record.serialize()
print(edm_xml)
```

### From the Command Line

```sh
python -m mets_to_edm example.xml "Provider Name" [--data-provider "Data Provider"]
```

- `"Provider Name"`: the institution name to be filled in as edm:provider (the aggregator providing the data to europeana)
- `"Data Provider"`: the institution name to be filled in as edm:dataProvider (the Organisation where the data originates from). Optional as it will otherwise be extracted from the amdSec using XPath "mets:rightsMD/mets:mdWrap/mets:xmlData/dv:rights/dv:owner"

## Customizing the Mapping

To change how specific fields are mapped, subclass `MetsToEdmMapper` and override the relevant class methods. For example, to change how titles are extracted:
You can override any method such as:
- `get_titles`
- `get_descriptions`
- `get_publishers`
- `get_types`
- `get_languages`
- ...and more (see `mets_to_edm/mapper.py` for all available hooks)

### Example: Overriding the Data Provider

```python
class MyMapper(MetsToEdmMapper):
	@classmethod
	def get_data_provider(cls, dmd_sec, amd_sec, default=None):
		return "My Custom Data Provider"

# Usage:
# edmlib_record = MyCustomMapper.process_record(tree)
```

For more examples have a look at the examples directory.

## Further Information

- See the source code in `mets_to_edm/mapper.py` for all overridable methods and mapping logic.
- For questions or contributions, open an issue or pull request.