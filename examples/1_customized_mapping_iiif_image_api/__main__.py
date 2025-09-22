from pathlib import Path

from lxml import etree

from customized_mapping import MetsModsMapperWithIIIF

dir_path = Path(__file__).resolve().parent
xml_tree = etree.parse(dir_path / "example-1.xml")

edmlib_record = MetsModsMapperWithIIIF.process_record(xml_tree)

with open(dir_path / "example-1-output.xml", "w", encoding="utf-8") as out_file:
    out_file.write(edmlib_record.serialize())
