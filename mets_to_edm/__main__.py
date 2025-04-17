import argparse
from lxml import etree
from mets_to_edm.mapper import MetsToEdmMapper


def main():
    parser = argparse.ArgumentParser(
        description="Process a file with a specified data provider."
    )
    parser.add_argument("file", type=str, help="Path to the input file")
    parser.add_argument("--data-provider", type=str, help="Name of the data provider")

    args = parser.parse_args()

    try:
        with open(args.file, "rb") as f:
            tree = etree.parse(f)
            print(
                MetsToEdmMapper.process_record(
                    tree, data_provider=args.data_provider
                ).serialize()
            )
    except (etree.XMLSyntaxError, FileNotFoundError) as e:
        print(f"Error parsing the file: {e}")


if __name__ == "__main__":
    main()
