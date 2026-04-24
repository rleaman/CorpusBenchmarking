import sys
import re

import bioc

handlers = {
    "BAO:": None,
    "CHEBI:": ("chemical", True),
    "CL:": ("cell_type", True),
    "CVCL_": ("cell_line", True),
    "Corum:": None, # Protein complexes
    "GO:": None,
    "NCBI gene:": ("gene", True),
    "NCBI taxon:": ("species", True),
    "PubChem:": ("chemical", True),
    "Rfam:": None, # RNA families
    "Uberon:": ("anatomy", True),
    "Uniprot:": ("gene", True),
    "cell:": ("cell_type", False),
    "gene:": ("gene", True),
    "molecule:": ("chemical", True),
    "organism:": ("species", True),
    "protein:": ("gene", True),
    "subcellular:": ("anatomy", True),
    "tissue:": ("anatomy", True),
}

def main():
    input_filenames = sys.argv[1:-1]
    output_filename = sys.argv[-1]

    output_collection = bioc.BioCCollection()
    for input_filename in input_filenames:
        print(f"Adding file {input_filename}")
        with open(input_filename, 'r') as fp:
            input_collection = bioc.load(fp)
        for document in input_collection.documents:
            for passage in document.passages:
                fix_annotations(document.id, passage)
            output_collection.add_document(document)
    with open(output_filename, 'w') as fp:
        bioc.dump(output_collection, fp)
    print("Done.")

def update_identifier(identifier):
    identifier = re.sub(r"^Uberon:UBERON:", "Uberon:UBERON_", identifier)
    return identifier

def infer_type_handler_from_identifier_list(identifier_list):
    identifiers = [update_identifier(id) for id in identifier_list.split("|")]
    handlers_found = set()
    for identifier in identifiers:
        found = 0
        for prefix, handler in handlers.items():
            if identifier.startswith(prefix):
                found += 1
                handlers_found.add(handler)
        if found != 1:
            print(f"WARN Identifier \"{identifier}\" matched {found} handlers")
    if len(handlers_found) != 1:
        print(f"WARN Identifier list \"{identifier_list}\" matched {len(handlers_found)} handlers")
    if len(handlers_found) == 0:
        return None, None
    handlers_found = list(handlers_found)
    handlers_found.sort()
    first = handlers_found[0]
    if first is None:
        return first, None
    type, keep_identifier = first
    if keep_identifier:
        return type, ",".join(identifiers)
    return type, None

def build_byte_to_char_list(text):
    byte_to_char = []
    for char_index, char in enumerate(text):
        for _ in char.encode('utf-8'):
            byte_to_char.append(char_index)
    return byte_to_char

def check_annotation_spans(passage, annotation):
    annotation_text = annotation.text
    location_texts = list()
    passage_text = passage.text
    passage_offset = passage.offset
    for location in annotation.locations:
        start = location.offset - passage_offset
        end = start + location.length
        location_texts.append(passage_text[start:end])
    location_text = " ... ".join(location_texts)
    if annotation_text == location_text:
        return 0
    else:
        print(f"WARN annotation {annotation.id} text is \"{annotation_text}\" but span(s) match \"{location_text}\"")
        return 1

def fix_annotations(document_id, passage):
    # Determine which annotations to keep
    annotations = list()
    b2c = build_byte_to_char_list(passage.text)
    for annotation in passage.annotations:
        # Infer how to handle the annotation, its type and identifier
        type, updated_identifier_list = infer_type_handler_from_identifier_list(annotation.infons["type"])
        if type is None:
            # Do not keep this annotation
            continue
        annotation.infons["type"] = type
        if not updated_identifier_list is None:
            annotation.infons["identifier"] = updated_identifier_list
        locations = annotation.locations
        if len(locations) != 1:
            print(f"WARN Annotation {document_id}:{annotation.id} has {len(locations)} locations, should be exactly 1")
            # Do not keep this annotation
            continue
        # Convert location from bytes to chars
        start_char = b2c[locations[0].offset]
        end_char = b2c[locations[0].end - 1] + 1  # inclusive range
        locations[0].offset = start_char
        locations[0].length = end_char - start_char
        location_text = passage.text[annotation.locations[0].offset:annotation.locations[0].end]
        if location_text != annotation.text:
            print(f"WARN annotation {document_id}:{annotation.id} text is \"{annotation.text}\" but span(s) match \"{location_text}\"")
            continue
        annotations.append(annotation)

    # Clear previous annotations and add fixed annotations
    passage.annotations.clear()
    annotations.sort(key=lambda x: x.locations[0].offset)
    for annotation in annotations:
        passage.annotations.append(annotation)

if __name__ == "__main__":
    main()