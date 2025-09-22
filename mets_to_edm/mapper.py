import logging
import os
from collections import defaultdict
from typing import Any, Callable, Optional, Type, Dict, List, Tuple, Union

from edmlib import (
    MixedValuesList,
    ORE_Aggregation,
    SKOS_Concept,
    EDM_Place,
    EDM_TimeSpan,
    EDM_Agent,
    EDM_WebResource,
    SVCS_Service,
)
from edmlib.edm import EDM_Record, EDM_ProvidedCHO, Lit, Ref
from lxml.etree import _Element

from .utilities import (
    METS_MODS_NAMESPACES,
    join_tag_texts_xpath,
    literal_list_from_xpath,
    xpath_first_match,
    mods_ns,
    uri_list_from_xpath,
    ModsNameResultsType,
    first_literal_from_xpath,
    CONTEXT_DICT_TYPE,
    context_dict_to_edm_record_dict,
)

logger = logging.getLogger("mets-to-edm")


XSL_FILE = os.path.join(os.path.dirname(__file__), "MODSMETS2EDM.xsl")


def retry_with_host_data(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper_retry_with_host_data(
        cls: Type["MetsToEdmMapper"],
        dmd_sec: _Element,
        host_dmd_sec: Optional[_Element] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if values := func(cls, dmd_sec=dmd_sec, *args, **kwargs):
            return values
        elif host_dmd_sec is not None:
            return func(cls, dmd_sec=host_dmd_sec, *args, **kwargs)
        else:
            return []

    return wrapper_retry_with_host_data


class MetsToEdmMapper:
    SUBJECT_SUBELEMENTS_MAPPING = {
        mods_ns("topic"): ("dc_subject", SKOS_Concept),
        mods_ns("geographic"): ("dcterms_spatial", EDM_Place),
        mods_ns("temporal"): ("dcterms_temporal", EDM_TimeSpan),
        mods_ns("titleInfo"): ("dc_subject", SKOS_Concept),
        mods_ns("name"): ("dc_subject", EDM_Agent),
        mods_ns("genre"): ("dc_type", SKOS_Concept),
        mods_ns("cartographics"): (None, None),
        mods_ns("hierarchicalGeographic"): (None, None),
        mods_ns("geographicCode"): (None, None),
        mods_ns("occupation"): (None, None),
        # TODO: maybe also support hierarchicalGeographic, cartographics, geographicCode, occupation
    }
    CREATOR_ROLES = ["aut", "cmp", "art", "pht", "edt"]
    PUBLISHER_ROLES = ["pbl", "isb"]
    SUBJECT_ROLES = ["rcp"]
    OTHER_ROLES = [
        "ctb",
        "trl",
        "prt",
        "oth",
        "egr",
        "cns",
        "ill",
        "chr",
        "wst",
        "dto",
        "asn",
        "lyr",
    ]
    IGNORE_ROLES = ["his"]

    # @classmethod
    # def get_file_from_logical_div(cls,record: _Element, logical_div: _Element):
    #    div_id = logical_div.get("ID")

    @classmethod
    def get_main_structmap_div(cls, record: _Element) -> _Element:
        possible_divs = record.xpath(
            "mets:structMap[@TYPE='LOGICAL']//mets:div[@DMDID and not(mets:mptr)]",
            namespaces=METS_MODS_NAMESPACES,
        )
        for div in possible_divs:
            # if div.get("TYPE") and div.get("TYPE").lower() in [
            #     "article",
            #     "issue",
            #     "volume",
            #     "document",
            #     "monograph",
            #     "multivolume_work",
            #     "multivolumework",
            # ]:
            return div
        # Else
        div = record.xpath(
            "(mets:structMap[@TYPE='LOGICAL']//mets:div[@DMDID])[1]",
            namespaces=METS_MODS_NAMESPACES,
        )
        assert len(div) > 0, "Could not find starting div in structmap"
        return div[0]

    @classmethod
    def get_mods_part(cls, record: _Element, dmdid: str) -> _Element:
        dmd_secs = record.xpath(
            f"mets:dmdSec[@ID='{dmdid}']/mets:mdWrap/mets:xmlData/mods:mods[1]",
            namespaces=METS_MODS_NAMESPACES,
        )
        assert len(dmd_secs) == 1, f"dmdsec not found or multiples for id {dmdid}"
        return dmd_secs[0]

    @classmethod
    def get_host_dmd_sec(
        cls, record: _Element, dmd_sec: _Element, logical_main_div: _Element
    ) -> Optional[_Element]:
        host_dmd_sec = None
        if possible_hosts := dmd_sec.xpath(
            "mods:relatedItem[@type='host']", namespaces=METS_MODS_NAMESPACES
        ):
            host_dmd_sec = possible_hosts[0]
        elif logical_host_div := logical_main_div.xpath(
            "ancestor::mets:div[@DMDID][1]", namespaces=METS_MODS_NAMESPACES
        ):
            host_dmd_sec = cls.get_mods_part(
                record, dmdid=logical_host_div[0].get("DMDID")
            )
        return host_dmd_sec

    @classmethod
    def get_amd_part(cls, record: _Element, amdid: str) -> list[_Element]:
        return record.xpath(
            f"mets:amdSec[@ID='{amdid}'][1]",
            namespaces=METS_MODS_NAMESPACES,
        )

    @classmethod
    def process_title_tag(cls, title_element: _Element) -> tuple[str, Lit]:
        # TODO: consider whitespace handling and separators
        title = join_tag_texts_xpath(title_element, "mods:nonSort")
        title += join_tag_texts_xpath(title_element, "mods:title")
        subtitle = join_tag_texts_xpath(title_element, "mods:subTitle", separator="; ")
        if subtitle:
            title += ": " + subtitle
        partnumber = join_tag_texts_xpath(
            title_element, "mods:partNumber", separator=", "
        )
        if partnumber:
            title += " " + partnumber
        partname = join_tag_texts_xpath(title_element, "mods:partName", separator=", ")
        if partname:
            title += ": " + partname

        # TODO: languages: either from attrs lang/xml:lang on titleInfo or subtags, or from document language

        if title_element.get("type"):
            return ("dcterms_alternative", Lit(value=title))
        else:
            return ("dc_title", Lit(value=title))

    @classmethod
    def get_titles(
        cls, dmd_sec: _Element, host_dmd_sec: Optional[_Element] = None
    ) -> Dict[str, List[Lit]]:
        title_properties = {"dcterms_alternative": [], "dc_title": []}
        titles = dmd_sec.xpath(
            "mods:titleInfo[not(@type)]", namespaces=METS_MODS_NAMESPACES
        )
        for title_info in titles:
            title_type, title = cls.process_title_tag(title_info)
            title_properties[title_type].append(title)

        if (
            not title_properties["dcterms_alternative"]
            and not title_properties["dc_title"]
        ):
            # If no title try to create it from host volume and part
            volume = None
            issue = None
            others = []
            detail_numbers = dmd_sec.xpath(
                "mods:part/mods:detail[mods:number]",
                namespaces=METS_MODS_NAMESPACES,
            )
            for detail_number in detail_numbers:
                number = detail_number.find(
                    "mods:number", namespaces=METS_MODS_NAMESPACES
                ).text
                if detail_number.get("type") == "volume":
                    volume = number
                elif detail_number.get("type") == "issue":
                    issue = number
                else:
                    others.append(number)

            if volume and issue:
                suffix = f"{volume}/{issue}"
            else:
                suffix = volume or issue or (others[0] if others else None)

            # suffix = dmd_sec.xpath(
            #    "mods:part/mods:detail/mods:number[1]/text()",
            #    namespaces=METS_MODS_NAMESPACES,
            # )
            if not suffix:
                date_suffix = dmd_sec.xpath(
                    "mods:part/mods:date[1]/text()", namespaces=METS_MODS_NAMESPACES
                )
                suffix = date_suffix[0] if date_suffix else None

            if suffix and host_dmd_sec is not None:
                for host_title_type, host_titles in cls.get_titles(
                    host_dmd_sec
                ).items():
                    for host_title in host_titles:
                        title_properties[host_title_type].append(
                            Lit(value=host_title.value + " " + suffix)
                        )
                return title_properties

            # if still no title try the mets:mets/@LABEL as last resort
            mets_label = dmd_sec.xpath(
                "/mets:mets/@LABEL", namespaces=METS_MODS_NAMESPACES
            )[0]
            if suffix and mets_label:
                title_properties["dc_title"].append(
                    Lit(value=mets_label + " " + suffix)
                )
        return title_properties

    @classmethod
    def get_descriptions(cls, dmd_sec: _Element) -> MixedValuesList:
        def note_string_extract(tag: _Element):
            output = ""
            if tag.get("type"):
                output += tag.get("type") + ": "
            output += tag.text
            return output

        return literal_list_from_xpath(
            dmd_sec, "mods:note", string_extract_function=note_string_extract
        ) + literal_list_from_xpath(dmd_sec, "mods:abstract")

    @classmethod
    def get_identifiers(cls, dmd_sec: _Element) -> List[Lit]:
        return literal_list_from_xpath(
            dmd_sec, "mods:recordInfo/mods:recordIdentifier"
        ) + literal_list_from_xpath(dmd_sec, "mods:identifier")

    @classmethod
    def get_edm_type(
        cls, dmd_sec: _Element, logical_main_div: Optional[_Element] = None
    ) -> Lit:
        return Lit(value="TEXT")

    @classmethod
    def parse_mods_subjects(
        cls, dmd_sec: _Element, context_objects: CONTEXT_DICT_TYPE
    ) -> Dict[str, List[Union[Lit, Ref]]]:
        subjects = dmd_sec.findall("mods:subject", namespaces=METS_MODS_NAMESPACES)
        edm_values: dict[str, list[Lit | Ref]] = defaultdict(list)
        for subject in subjects:
            for subject_subelement in subject:
                edm_property, context_class = cls.SUBJECT_SUBELEMENTS_MAPPING[
                    subject_subelement.tag
                ]
                if edm_property is None:
                    logger.warning(
                        f"unimplemented mods:subject subelement {subject_subelement.tag}"
                    )
                    continue
                if subject_subelement.tag == mods_ns("titleInfo"):
                    pref_label = cls.process_title_tag(subject_subelement)[1]
                elif subject_subelement.tag == mods_ns("name"):
                    person = cls.parse_mods_name(subject_subelement)
                    if isinstance(person, EDM_Agent):
                        context_objects[person.id.value] = person
                        edm_values[edm_property].append(person.id)
                        continue
                    else:
                        pref_label = Lit(value=person.value)
                else:
                    pref_label = Lit(value=subject_subelement.text)

                if subject_subelement.get("valueURI"):
                    context_object = context_class(
                        id=Ref(value=subject_subelement.get("valueURI")),
                        skos_prefLabel=[pref_label],
                    )
                    context_objects[context_object.id.value] = context_object
                    edm_values[edm_property].append(context_object.id)
                else:
                    edm_values[edm_property].append(pref_label)
        return edm_values

    @classmethod
    def get_subjects(cls, dmd_sec: _Element) -> MixedValuesList:
        # intranda extension:
        return literal_list_from_xpath(
            dmd_sec, "mods:extension/intranda:intranda/intranda:subjectPerson"
        ) + literal_list_from_xpath(dmd_sec, "mods:extension/intranda:Topic")

    @classmethod
    def parse_logical_main_div_type(
        cls, logical_main_div: Optional[_Element] = None
    ) -> List[Lit]:
        if logical_main_div is not None and logical_main_div.get("TYPE"):
            type_from_div = [Lit(value=logical_main_div.get("TYPE"))]
            return type_from_div
        else:
            return []

    @classmethod
    def get_types(
        cls, dmd_sec: _Element, logical_main_div: Optional[_Element] = None
    ) -> MixedValuesList:
        # intranda extension:
        return (
            literal_list_from_xpath(dmd_sec, "mods:extension/intranda:ObjectType")
            + literal_list_from_xpath(
                dmd_sec, "mods:physicalDescription/mods:form[@type='technique']"
            )
            + literal_list_from_xpath(dmd_sec, "mods:genre")
            + cls.parse_logical_main_div_type(logical_main_div)
        )

    @classmethod
    def get_temporals(cls, dmd_sec: _Element) -> MixedValuesList:
        # intranda extension:
        return literal_list_from_xpath(dmd_sec, "mods:extension/intranda:TopicPeriod")

    @classmethod
    def get_spatials(cls, dmd_sec: _Element) -> MixedValuesList:
        # intranda extension:
        intranda_spatials = literal_list_from_xpath(
            dmd_sec, "mods:extension/intranda:TopicRoom"
        )
        origin_places = literal_list_from_xpath(
            dmd_sec, "mods:originInfo/mods:place/mods:placeTerm[@type='text']"
        )
        return intranda_spatials + origin_places

    @classmethod
    def get_mediums(cls, dmd_sec: _Element) -> MixedValuesList:
        return literal_list_from_xpath(
            dmd_sec,
            "mods:physicalDescription/mods:form[not(@type='technique') and not(@type='dimensions')]",
        )  # TODO: check if there is a valueURI to create vocabulary references

    @classmethod
    def get_extent(cls, dmd_sec: _Element) -> MixedValuesList:
        return literal_list_from_xpath(
            dmd_sec, "mods:physicalDescription/mods:extent"
        ) + literal_list_from_xpath(
            dmd_sec, "mods:physicalDescription/mods:form[@type='dimensions']"
        )

    @classmethod
    def get_languages(cls, dmd_sec: _Element) -> List[str]:
        langs = dmd_sec.xpath(
            "mods:language/mods:languageTerm/text()", namespaces=METS_MODS_NAMESPACES
        )
        # TODO: convert to ISO language codes
        return langs

    @classmethod
    def parse_mods_date(
        cls, dmd_sec: _Element, date_element_name: str
    ) -> Optional[List[Lit]]:
        dates = dmd_sec.xpath(date_element_name, namespaces=METS_MODS_NAMESPACES)
        start = ""
        end = ""
        other = ""
        for date in dates:
            if date.get("point") == "start":
                start = date.text
            elif date.get("point") == "end":
                end = date.text
            else:
                if date.get("keyDate") == "yes" or not other:
                    other = date.text
        # TODO: consider other date attributes, like qualifier for approximate/inferred/questionable
        if start and end:
            return [Lit(value=start + "-" + end)]
        elif other:
            return [Lit(value=other)]
        elif start or end:
            return [Lit(value=start + "-" + end)]
        else:
            return None

    @classmethod
    def get_issued(cls, dmd_sec: _Element) -> Optional[List[Lit]]:
        return cls.parse_mods_date(dmd_sec, "mods:originInfo/mods:dateIssued")

    @classmethod
    def get_created(cls, dmd_sec: _Element) -> Optional[List[Lit]]:
        return cls.parse_mods_date(dmd_sec, "mods:originInfo/mods:dateCreated")

    @classmethod
    @retry_with_host_data
    def get_publishers(
        cls, dmd_sec: _Element, host_dmd_sec: Optional[_Element] = None
    ) -> List[Lit]:
        return literal_list_from_xpath(dmd_sec, "mods:originInfo/mods:publisher")

    @classmethod
    def get_full_name_from_name_tag(cls, name_tag: _Element) -> str:
        # first try displayForm
        display_form = name_tag.find(
            "mods:displayForm", namespaces=METS_MODS_NAMESPACES
        )
        if display_form is not None and display_form.text:
            return display_form.text

        # otherwise join nameparts based on type
        given_name = join_tag_texts_xpath(
            name_tag, "mods:namePart[@type='given']", separator=" "
        )
        family_name = join_tag_texts_xpath(
            name_tag, "mods:namePart[@type='family']", separator=" "
        )
        address = join_tag_texts_xpath(
            name_tag, "mods:namePart[@type='termsOfAddress']", separator=" "
        )
        name = (" ".join([given_name, family_name, address])).strip()
        if name:
            return name

        # otherwise use nameparts without type
        return join_tag_texts_xpath(
            name_tag, "mods:namePart[not(@type)]", separator=" "
        )

    @classmethod
    def parse_mods_name(cls, name_tag: _Element) -> Union[Lit, EDM_Agent]:
        uri = name_tag.get("valueURI")
        if not uri:
            uri = name_tag.get("nameIdentifier")

        # name
        name = Lit(value=cls.get_full_name_from_name_tag(name_tag))

        # then do alternativeNames as well
        alt_names = [
            Lit(value=cls.get_full_name_from_name_tag(alt_name_tag))
            for alt_name_tag in name_tag.findall(
                "mods:alternativeName", namespaces=METS_MODS_NAMESPACES
            )
        ]
        # TODO: maybe also support altRepGroup in the future

        if not alt_names and not uri:
            return name
        else:
            if not uri:
                uri = "agent"
            return EDM_Agent(
                id=Ref(value=uri), skos_prefLabel=[name], skos_altLabel=alt_names
            )

    @classmethod
    def get_edm_property_for_roles(cls, roles: List[str]) -> Optional[str]:
        edm_property = "dc_contributor"
        for role_entry in roles:
            if role_entry in cls.IGNORE_ROLES:
                return None
            elif role_entry in cls.CREATOR_ROLES:
                return "dc_creator"
            elif role_entry in cls.PUBLISHER_ROLES:
                edm_property = "dc_publisher"
            elif role_entry in cls.SUBJECT_ROLES:
                edm_property = "dc_subject"
            elif role_entry not in cls.OTHER_ROLES:
                logger.warning(
                    f'Unknown Role: "{role_entry}", falling back to contributor'
                )
        return edm_property

    @classmethod
    def parse_mods_names(
        cls, dmd_sec: _Element, context_objects: CONTEXT_DICT_TYPE
    ) -> ModsNameResultsType:
        name_results = {
            "dc_creator": [],
            "dc_publisher": [],
            "dc_contributor": [],
            "dcterms_provenance": [],
            "dc_subject": [],
        }
        for name_tag in dmd_sec.findall("mods:name", namespaces=METS_MODS_NAMESPACES):
            literal_or_agent = cls.parse_mods_name(name_tag)
            name_value = literal_or_agent
            if isinstance(literal_or_agent, EDM_Agent):
                context_objects[literal_or_agent.id.value] = literal_or_agent
                name_value = literal_or_agent.id

            roles = [
                r.text
                for r in name_tag.findall(
                    "mods:role/mods:roleTerm", namespaces=METS_MODS_NAMESPACES
                )
            ]
            if "fmo" in roles:
                former_owner_value = (
                    literal_or_agent.skos_prefLabel[0].value
                    if isinstance(literal_or_agent, EDM_Agent)
                    else literal_or_agent.value
                )
                name_results["dcterms_provenance"] += [
                    Lit(
                        value="Former owner: " + former_owner_value,
                        lang="en",
                    ),
                    Lit(
                        value="Frühere:r Eigentümer:in: " + former_owner_value,
                        lang="de",
                    ),
                ]
                roles.remove("fmo")
                if not roles:
                    break

            edm_property = cls.get_edm_property_for_roles(roles)
            if edm_property:
                name_results[edm_property].append(name_value)
        return name_results

    @classmethod
    def get_edm_rights(cls, dmd_sec: _Element) -> Ref:
        access_conditions = dmd_sec.xpath(
            "mods:accessCondition[@xlink:href][1]/@xlink:href",
            namespaces=METS_MODS_NAMESPACES,
        )
        if access_conditions:
            return Ref(value=access_conditions[0].replace("https://", "http://"))

        access_conditions = dmd_sec.xpath(
            "mods:accessCondition[@mods:valueURI][1]/@mods:valueURI",
            namespaces=METS_MODS_NAMESPACES,
        )
        if access_conditions:
            return Ref(value=access_conditions[0].replace("https://", "http://"))

        access_conditions = dmd_sec.xpath(
            "mods:accessCondition[@type!='hide']", namespaces=METS_MODS_NAMESPACES
        )
        if access_conditions:
            return Ref(
                value=access_conditions[0].text.strip().replace("https://", "http://")
            )

        raise Exception("no corresponding field for edm:rights found")

    @classmethod
    def get_data_provider(
        cls, dmd_sec: _Element, amd_sec: _Element, default: Optional[str] = None
    ) -> Lit:
        if default:
            return Lit(value=default)

        data_provider = amd_sec.find(
            "mets:rightsMD/mets:mdWrap/mets:xmlData/dv:rights/dv:owner",
            namespaces=METS_MODS_NAMESPACES,
        )
        return Lit(value=data_provider.text)

    @classmethod
    def get_provider(cls, default: Optional[str]) -> Lit:
        assert (
            default
        ), "Missing value for edm:provider. Either override get_provider or provide a default value to process_record."
        return Lit(value=default)

    @classmethod
    def get_is_part_of(cls, dmd_sec: _Element) -> List[Any]:
        return []

    @classmethod
    def get_referenced_by(
        cls, dmd_sec: _Element, contex_objects: CONTEXT_DICT_TYPE
    ) -> MixedValuesList:
        return []

    @classmethod
    def get_current_location(cls, dmd_sec: _Element) -> Optional[Lit]:
        location = join_tag_texts_xpath(
            dmd_sec, "mods:location[1]/mods:physicalLocation[1]"
        )
        shelf_locator = join_tag_texts_xpath(
            dmd_sec, "mods:location[1]/mods:shelfLocator", separator=" ; "
        )
        full_location = location + ((" ; " + shelf_locator) if shelf_locator else "")
        return Lit(value=full_location) if full_location else None

    @classmethod
    def get_iiif_image_api_service(cls, url: str) -> Optional[SVCS_Service]:
        """Override in Institution specific implementation to generate the SVCS_Service object from a given url

        Args:
            url: URL of a WebResource

        Returns:
            SVCS_Service object or None if no IIIF Image API service can be extracted from the url
        """
        return None

    @classmethod
    def get_iiif_manifest_url(cls, amd_sec: _Element) -> Optional[List[Ref]]:
        iiif_manifest = amd_sec.find(
            "mets:digiprovMD/mets:mdWrap/mets:xmlData/dv:links/dv:iiif",
            namespaces=METS_MODS_NAMESPACES,
        )
        if iiif_manifest is None:
            return None
        return [Ref(value=iiif_manifest.text)]

    @classmethod
    def query_url_for_div(
        cls, div: _Element, file_sec: _Element, file_grp: str
    ) -> Optional[str]:
        fptr_id = xpath_first_match(
            div,
            f"mets:fptr[contains(@FILEID,'{file_grp}')]/@FILEID",
        )
        # assert fptr_id, "no fptr found"
        if fptr_id:
            file_url = xpath_first_match(
                file_sec,
                f".//mets:file[@ID='{fptr_id}'][1]/mets:FLocat[@LOCTYPE='URL']/@xlink:href",
            )
            assert file_url, f"file with ID {fptr_id} not found"
            return file_url
        return None

    @classmethod
    def query_shownBy_urls(
        cls,
        physical_div: _Element,
        file_sec: _Element,
        xpath_query_pages: str = "mets:div[@TYPE='page']",
        file_grp: str = "DEFAULT",
    ) -> List[str]:
        urls = []
        if physical_div is not None:
            page_divs = physical_div.xpath(
                xpath_query_pages, namespaces=METS_MODS_NAMESPACES
            )
            for page_div in page_divs:
                # TODO: consider ORDER attributes on page divs
                file_url = cls.query_url_for_div(page_div, file_sec, file_grp)
                if file_url:
                    urls.append(file_url)
        return urls

    @classmethod
    def get_object(cls, logical_div: _Element, file_sec: _Element) -> Optional[Ref]:
        thumbnail_id = (
            xpath_first_match(
                logical_div, "mets:fptr[contains(@FILEID,'FRONTIMAGE')]/@FILEID"
            )
            or xpath_first_match(
                logical_div, "mets:fptr[contains(@FILEID,'TEASER')]/@FILEID"
            )
            or xpath_first_match(
                file_sec, "mets:fileGrp[@USE='DEFAULT']/mets:file[@USE='banner']/@ID"
            )
        )
        # TODO: last option: get from TitlePage
        if thumbnail_id is None:
            return None
        thumbnail_url = xpath_first_match(
            file_sec,
            f".//mets:file[@ID='{thumbnail_id}'][1]/mets:FLocat[@LOCTYPE='URL']/@xlink:href",
        )
        return Ref(value=thumbnail_url)

    @classmethod
    def get_webresource_urls(
        cls,
        amd_sec: _Element,
        physical_div: _Element,
        logical_div: _Element,
        file_sec: _Element,
        context_objects: CONTEXT_DICT_TYPE,
    ) -> Dict[str, Any]:
        results = {
            "edm_hasView": [],
            "edm_isShownBy": None,
            "edm_isShownAt": None,
            "edm_object": None,
        }

        urls = cls.query_shownBy_urls(physical_div, file_sec)

        edm_object = cls.get_object(logical_div, file_sec)
        results["edm_object"] = edm_object
        if edm_object and not urls:
            urls.append(edm_object.value)

        iiif_manifest = cls.get_iiif_manifest_url(amd_sec)

        first = True
        for url in urls:
            url = url.replace(" ", "%20")
            service = cls.get_iiif_image_api_service(url)
            has_service = None
            if service:
                context_objects[service.id.value] = service
                has_service = [service.id]
            if iiif_manifest or service:
                context_objects[url] = EDM_WebResource(
                    id=Ref(value=url),
                    dcterms_isReferencedBy=iiif_manifest,
                    svcs_has_service=has_service,
                )
            if first:
                results["edm_isShownBy"] = Ref(value=url)
                first = False
            else:
                results["edm_hasView"].append(Ref(value=url))

        if pdf_url := cls.query_url_for_div(logical_div, file_sec, file_grp="PDF"):
            if first:
                results["edm_isShownBy"] = Ref(value=pdf_url)
                first = False
            else:
                results["edm_hasView"].append(Ref(value=pdf_url))

        shown_ats = uri_list_from_xpath(
            amd_sec,
            "mets:digiprovMD/mets:mdWrap/mets:xmlData/dv:links/dv:presentation",
        )
        results["edm_isShownAt"] = shown_ats[0]
        if len(shown_ats) > 1:
            results["edm_hasView"] += shown_ats[1:]
        # TODO: maybe if there is a "mods:originInfo/mods:dateCaptured" put it into the WebResource as dcterms:created
        return results

    @classmethod
    def process_record(
        cls,
        record: _Element,
        edm_provider: Optional[str] = None,
        data_provider: Optional[str] = None,
    ) -> EDM_Record:
        """Maps a METS/MODS record to EDM using class methods that can be overwritten to adapt the mapping logic

        Args:
            record: METS/MODS record as lxml Element or ElementTree
            edm_provider: default value for edm:provider (Institution providing the data to Europeana). Mandatory if not overwritten in a subclass by overriding get_provider.
            data_provider: default value for edm:dataProvider (Institution providing the original data), if not provided in the METS/MODS record. Would otherwise be extracted from the amdSec using "mets:rightsMD/mets:mdWrap/mets:xmlData/dv:rights/dv:owner"
        """

        context_objects: CONTEXT_DICT_TYPE = {}

        record = record.xpath("//mets:mets", namespaces=METS_MODS_NAMESPACES)[0]
        logical_main_div = cls.get_main_structmap_div(record)
        dmd_sec = cls.get_mods_part(record, dmdid=logical_main_div.get("DMDID"))
        host_dmd_sec = cls.get_host_dmd_sec(record, dmd_sec, logical_main_div)

        amd_sec = cls.get_amd_part(record, amdid=logical_main_div.get("ADMID"))
        assert (
            len(amd_sec) == 1
        ), f'amdsec not found or multiples for id {logical_main_div.get("ADMID")}'
        amd_sec = amd_sec[0]
        physical_main_div = record.find(
            "mets:structMap[@TYPE='PHYSICAL']/mets:div",  # [@TYPE='physSequence']",
            namespaces=METS_MODS_NAMESPACES,
        )

        filesec = record.find("mets:fileSec", namespaces=METS_MODS_NAMESPACES)

        edm_type = cls.get_edm_type(dmd_sec, logical_main_div=logical_main_div)

        languages = cls.get_languages(dmd_sec)
        lang_tag = languages[0] if len(languages) == 1 else None
        if len(languages) == 0 and edm_type.value == "TEXT":
            languages = ["und"]

        titles = cls.get_titles(dmd_sec, host_dmd_sec=host_dmd_sec)

        # TODO: lang tags potentially for all properties

        from_mods_subject = cls.parse_mods_subjects(dmd_sec, context_objects)

        from_mods_name = cls.parse_mods_names(dmd_sec, context_objects)

        cho = EDM_ProvidedCHO(
            id=Ref(value="1"),  # TODO: id
            **titles,
            dc_description=cls.get_descriptions(dmd_sec),
            edm_type=edm_type,
            dc_language=[Lit(value=lang) for lang in languages],
            dc_type=cls.get_types(dmd_sec, logical_main_div)
            + from_mods_subject["dc_type"],
            dc_subject=cls.get_subjects(dmd_sec)
            + from_mods_subject["dc_subject"]
            + from_mods_name["dc_subject"],
            dcterms_temporal=cls.get_temporals(dmd_sec)
            + from_mods_subject["dcterms_temporal"],
            dcterms_spatial=cls.get_spatials(dmd_sec)
            + from_mods_subject["dcterms_spatial"],
            dc_identifier=cls.get_identifiers(dmd_sec),
            dcterms_medium=cls.get_mediums(dmd_sec),
            dcterms_extent=cls.get_extent(dmd_sec),
            dc_publisher=cls.get_publishers(dmd_sec, host_dmd_sec=host_dmd_sec)
            + from_mods_name["dc_publisher"],
            dc_creator=from_mods_name["dc_creator"],
            dc_contributor=from_mods_name["dc_contributor"],
            dcterms_provenance=from_mods_name["dcterms_provenance"],
            dcterms_issued=cls.get_issued(dmd_sec),
            dcterms_created=cls.get_created(dmd_sec),
            dcterms_isPartOf=cls.get_is_part_of(dmd_sec),
            dcterms_isReferencedBy=cls.get_referenced_by(dmd_sec, context_objects),
            edm_currentLocation=cls.get_current_location(dmd_sec),
        )

        provider = cls.get_provider(default=edm_provider)
        aggregation = ORE_Aggregation(
            id=Ref(value="2"),  # TODO: id
            edm_rights=cls.get_edm_rights(dmd_sec),
            edm_aggregatedCHO=cho.id,
            edm_dataProvider=cls.get_data_provider(
                dmd_sec, amd_sec, default=data_provider
            ),
            edm_provider=provider,
            **cls.get_webresource_urls(
                amd_sec, physical_main_div, logical_main_div, filesec, context_objects
            ),
        )

        context_classes = context_dict_to_edm_record_dict(context_objects)

        return EDM_Record(provided_cho=cho, aggregation=aggregation, **context_classes)
