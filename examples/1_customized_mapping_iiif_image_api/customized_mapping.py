import re
from typing import Optional

from edmlib import Lit, MixedValuesList, Ref, SVCS_Service
from mets_to_edm.mapper import MetsToEdmMapper
from lxml.etree import _Element


class MetsModsMapperWithIIIF(MetsToEdmMapper):
    """Example of a customized mapping class with the following changes:
    - Only identifiers starting with "urn:" are kept
    - Descriptions are not mapped
    - IIIF Image API service is extracted from a specific URL pattern for each url
    """

    url_pattern = re.compile(
        r"https?://www.digital.wienbibliothek.at/download/webcache/(?:304|1000)/(.+)$"
    )

    @classmethod
    def get_identifiers(cls, dmd_sec: _Element) -> MixedValuesList:
        return [
            id for id in super().get_identifiers(dmd_sec) if id.value.startswith("urn:")
        ]

    @classmethod
    def get_iiif_image_api_service(cls, url) -> SVCS_Service | None:
        image_id = cls.url_pattern.search(url).group(1)
        return SVCS_Service(
            id=Ref(value=f"https://www.digital.wienbibliothek.at/i3f/v20/{image_id}"),
            dcterms_conformsTo=[Ref(value="http://iiif.io/api/image")],
            doap_implements=Ref(value="http://iiif.io/api/image/2/level2.json"),
        )

    @classmethod
    def get_descriptions(cls, dmd_sec: _Element) -> MixedValuesList:
        return []

    @classmethod
    def get_provider(cls, default: Optional[str] = None) -> Lit:
        return Lit(value="Kulturpool")
