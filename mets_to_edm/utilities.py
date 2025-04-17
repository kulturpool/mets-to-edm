from typing import TypedDict, Callable

from edmlib import EDM_TimeSpan, EDM_WebResource, MixedValuesList
from edmlib.edm import Lit, Ref
from edmlib.edm.base import EDM_BaseClass
from lxml.etree import _Element

CONTEXT_DICT_TYPE = dict[str, EDM_BaseClass]


def context_dict_to_edm_record_dict(
    context_objects: CONTEXT_DICT_TYPE,
) -> dict[str, list[EDM_BaseClass]]:
    context_classes = {
        "edm_agent": [],
        "edm_place": [],
        "edm_time_span": [],
        "skos_concept": [],
        "svcs_service": [],
        "web_resource": [],
    }
    for context_object in context_objects.values():
        if isinstance(context_object, EDM_WebResource):
            context_classes["web_resource"].append(context_object)
        elif isinstance(context_object, EDM_TimeSpan):
            context_classes["edm_time_span"].append(context_object)
        else:
            context_classes[type(context_object).__name__.lower()].append(
                context_object
            )
    return context_classes


METS_MODS_NAMESPACES = {
    "mets": "http://www.loc.gov/METS/",
    "mods": "http://www.loc.gov/mods/v3",
    "intranda": "http://intranda.com/MODS/",
    "xlink": "http://www.w3.org/1999/xlink",
    "dv": "http://dfg-viewer.de/",
    "vl": "http://visuallibrary.net/vl",
    "ext": "http://ns.vls.io/mods",
}

ModsNameResultsType = TypedDict(
    "ModsNameResultsType",
    {
        "dc_creator": MixedValuesList,
        "dc_publisher": MixedValuesList,
        "dc_contributor": MixedValuesList,
        "dcterms_provenance": MixedValuesList,
        "dc_subject": MixedValuesList,
    },
)


def xpath_first_match(element: _Element, xpath_query):
    results = element.xpath(xpath_query, namespaces=METS_MODS_NAMESPACES)
    return results[0] if results else None


def join_tag_texts(elements: list[_Element], separator=" "):
    if len(elements) > 0:
        return separator.join([element.text for element in elements if element.text])
    else:
        return ""


def join_tag_texts_xpath(element: _Element, xpath_query, separator=" "):
    return join_tag_texts(
        element.xpath(xpath_query, namespaces=METS_MODS_NAMESPACES),
        separator=separator,
    )


def literal_list_from_xpath(
    element: _Element,
    xpath_query,
    string_extract_function: Callable[[_Element], str] = None,
):
    return [
        Lit(
            value=string_extract_function(tag) if string_extract_function else tag.text,
            lang=tag.get("lang"),
        )
        for tag in element.xpath(xpath_query, namespaces=METS_MODS_NAMESPACES)
    ]


def first_literal_from_xpath(element: _Element, xpath_query):
    literal_list = literal_list_from_xpath(element, xpath_query)
    if literal_list:
        return literal_list[0]
    return None


def uri_list_from_xpath(element: _Element, xpath_query):
    return [
        Ref(value=tag.text)
        for tag in element.xpath(xpath_query, namespaces=METS_MODS_NAMESPACES)
    ]


def mods_ns(tag_name: str):
    return "{" + METS_MODS_NAMESPACES["mods"] + "}" + tag_name
