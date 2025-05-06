import re
import unidecode
from rdflib import Graph, RDFS, Literal
from difflib import SequenceMatcher

def remove_spaces(value:str):
    return value.replace(" ", "")

def split_cell_content(cell_content:str, sep:str=",", remove_spaces:bool=True):
    if cell_content == "" or cell_content is None:
        return []
    
    elems = cell_content.split(sep)
    if remove_spaces:
        return [re.sub("(^ {1,}| {1,}$)", "", x) for x in elems]
    return elems

def remove_abbreviations_from_dict(name:str, abbreviations_dict:dict, entire_match:bool=False):
    normalized_name = name
    for abbre, val in abbreviations_dict.items():
        if entire_match:
            abbre = f"^{abbre}$"
        normalized_name = re.sub(abbre, val, normalized_name)

    return normalized_name

def match_apostrophe(matchobj:re.Match):
    to_replace = re.sub("'{1,}", " ", matchobj.group(0))
    return to_replace

def get_words_list_from_label(label:str, case_option:str=None):
    split_setting = " "
    separated_words = re.sub("[’'ʼ]", "'", label) # Replace apostrophies by only one version
    separated_words = re.sub("[- ]{1,}", " ", label) # Replace dash and spaces by `split_setting`
    separated_words = re.sub("(^| )[a-z]('{1,})", match_apostrophe, separated_words, flags=re.IGNORECASE)

    if case_option == "lower":
        separated_words = separated_words.lower()
    elif case_option == "upper":
        separated_words = separated_words.upper()
    elif case_option == "title":
        separated_words = separated_words.title()
    elif case_option == "capitalize":
        separated_words = separated_words.capitalize()

    words_list = separated_words.split(split_setting)
    return words_list

def normalize_french_commune_name(commune_name:str):
    abbreviations_dict = {"st(\.|)":"saint", "ste(\.|)":"sainte", "sts(\.|)":"saints", "stes(\.|)":"saintes",
                         "chap(\.|)":"chapelle",
                         "gd":"grand", "pt":"petit", "vx":"vieux", 
                        }
    lower_case_words = ["à","au","aux","chez","d","de","derrière","des","dessous","dessus","deux","devant","du","en","entre","ès","et","l","la","le","les","lès","près","sous","sur"]
    words_before_apostrophe = ["d","l"]

    commune_name_words = get_words_list_from_label(commune_name, case_option="lower")
    for i, raw_word in enumerate(commune_name_words):
        if raw_word in lower_case_words:
            is_lower_case_word = True
        else:
            is_lower_case_word = False

        if not is_lower_case_word or i == 0 or next_chr == " ":
            word = remove_abbreviations_from_dict(raw_word, abbreviations_dict, True)
            word = word.capitalize()
        else:
            word = raw_word

        next_chr = "-"
        if i == len(commune_name_words) - 1:
            next_chr = ""
        elif raw_word in words_before_apostrophe:
            next_chr = "'"
        elif i == 0 and is_lower_case_word :
            next_chr = " "
        
        commune_name_words[i] = word + next_chr
   
    return "".join(commune_name_words)

def normalize_french_thoroughfare_name(thoroughfare_name:str):
    abbreviations_dict = {
        "pl(a|)(\.|)":"place",
        "av(\.|)":"avenue",
        "(bd|blvd|boul)(\.|)":"boulevard",
        "bre(\.|)":"barrière",
        "barriere":"barrière",
        "crs(\.|)":"cours",
        "r(\.|)":"rue",
        "rl{1,2}e(\.|)":"ruelle",
        "rte(\.|)":"route",
        "pas(s|)(\.|)": "passage",
        "all(ee|)(\.|)": "allée",
        "imp(\.|)": "impasse",
        "mte(\.|)": "montée",
        "montee ": "montée",
        "r(e|é)s(idence)(\.|)": "résidence",
        "s(\.|)":"saint", "st(\.|)":"saint", "ste(\.|)":"sainte", "sts(\.|)":"saints", "stes(\.|)":"saintes",
        "mlle(\.|)":"mademoiselle",
        "mme(\.|)":"madame",
        "fg(\.|)":"faubourg",
        "rpt(\.|)":"rond-point",
        "gd(\.|)":"grand",
        "gds(\.|)":"grands",
        "gde(\.|)":"grande",
        "gdes(\.|)":"grandes",
        "pt(\.|)":"petit",
        "pte(\.|)":"petite",
        "pts(\.|)":"petits",
        "vx(\.|)":"vieux",
        "0":"zéro", "1":"un", "2":"deux", "3":"trois", "4":"quatre", "5":"cinq", "6":"six", "7":"sept", "8":"huit", "9":"neuf",
        "10":"dix", "11":"onze", "12":"douze", "13":"treize", "14":"quatorze", "15":"quinze", "16":"seize", "17":"dix-sept", "18":"dix-huit", "19":"dix-neuf",
        "20":"vingt", "30":"trente", "40":"quarante", "50":"cinquante", "60":"soixante", "70":"soixante-dix", "80":"quatre-vingts", "90":"quatre-vingt-dix",
        "100":"cent", "1000":"mille",
    }

    lower_case_words = ["&","a","à","au","aux","d","de","des","du","en","ès","es","et","l","la","le","les","lès","ou","sous","sur"]
    words_before_apostrophe = ["d","l"]
        
    thoroughfare_name_words = get_words_list_from_label(thoroughfare_name, case_option="lower")

    for i, word in enumerate(thoroughfare_name_words):
        word = remove_abbreviations_from_dict(word, abbreviations_dict, True)
        if word not in lower_case_words:
            word = word.capitalize()
        if word in words_before_apostrophe:
            word += "'"
        thoroughfare_name_words[i] = word
    
    normalized_name = " ".join(thoroughfare_name_words)
    normalized_name = normalized_name.replace("' ", "'")
    return normalized_name


def simplify_french_landmark_name(landmark_name:str, keep_spaces:bool=True, keep_diacritics:bool=True, sort_characters:bool=False):
    words_to_remove = ["&","a","à","au","aux","d","de","des","du","en","ès","es","et","l","la","le","les","lès","ou"]
    words_to_replace = {
        "boulevart":"boulevard",
        "quay":"quai",
        "enfans":"enfants",
        "fauxbourg":"faubourg",
    }
    commune_name_words = get_words_list_from_label(landmark_name, case_option="lower")
    new_commune_name_words = []

    for word in commune_name_words:
        word = word.replace("'", "")
        if word not in words_to_remove:
            if not keep_diacritics:
                word = unidecode.unidecode(word)
            new_commune_name_words.append(word)

    for word, replacement in words_to_replace.items():
        new_commune_name_words = [replacement if x == word else x for x in new_commune_name_words]

    word_sep = ""
    if keep_spaces:
        word_sep = " "

    simplified_name = word_sep.join(new_commune_name_words)

    if sort_characters:
        simplified_name = "".join(sorted(simplified_name))

    return simplified_name

def get_remplacement_sparql_function(string:str, replacements:list):
    function_str = string
    for repl in replacements:
        arg, pattern, replacement = function_str, repl[0], repl[1]
        pattern = pattern.replace('\\', '\\\\')
        function_str = f"REPLACE({arg}, \"{pattern}\", \"{replacement}\")"

    return function_str

def normalize_street_rdfs_labels_in_graph_file(graph_file:str):
    # Standardisation of road names
    g = Graph()
    g.parse(graph_file)
    triples_to_remove = []
    triples_to_add = []
    for s, p, o in g:
        if p == RDFS.label and isinstance(o, Literal) and o.language == "fr":
            new_o_value = normalize_french_thoroughfare_name(o.value)
            new_o = Literal(new_o_value, lang="fr")
            triples_to_remove.append((s,p,o))
            triples_to_add.append((s, p, new_o))

    for triple in triples_to_remove:
        g.remove(triple)
    for triple in triples_to_add:
        g.add(triple)

    g.serialize(graph_file)

def are_similar_names(name_1:str, name_2:str, min_score:float=0.9):
    similarity_score = SequenceMatcher(None, name_1, name_2).ratio()
    if similarity_score >= min_score:
        return True
    return False

def normalize_french_name_version(name_version:str, name_type:str):
    if name_type == "number":
        return name_version.lower()
    elif name_type == "thoroughfare":
        return normalize_french_thoroughfare_name(name_version)
    elif name_type == "area":
        return normalize_french_commune_name(name_version)
    else:
        return None
    
def normalize_nolang_name_version(name_version:str, name_type:str):
    if name_type == "number":
        return name_version.lower()
    else:
        return None
    
def normalize_name_version(name_version:str, name_type:str, name_lang:str):
    if name_version is None:
        return None
    elif name_lang is None:
        return normalize_nolang_name_version(name_version, name_type)
    elif name_lang == "fr":
        return normalize_french_name_version(name_version, name_type)
    
    return None

def simplify_french_name_version(name_version:str, name_type:str):
    if name_type == "number":
        return remove_spaces(name_version)
    elif name_type in ["thoroughfare", "area"]:
        return simplify_french_landmark_name(name_version, keep_spaces=False, keep_diacritics=False, sort_characters=False)
    else:
        return None

def simplify_nolang_name_version(name_version:str, name_type:str):
    if name_type == "number":
        return remove_spaces(name_version)
    return None

def simplify_name_version(name_version:str, name_type:str, name_lang:str):
    if name_version is None:
        return None
    if name_lang is None:
        return simplify_nolang_name_version(name_version, name_type)
    elif name_lang == "fr":
        return simplify_french_name_version(name_version, name_type)
    
    return None

def normalize_and_simplify_name_version(name_version:str, name_type:str, name_lang:str):
    normalized_name = normalize_name_version(name_version, name_type, name_lang)
    simplified_name = simplify_name_version(normalized_name, name_type, name_lang)

    return normalized_name, simplified_name

def split_french_address(address):
    # Regex améliorée pour capturer le numéro avec ses variantes
    match = re.match(r"^(\d+\s*(?:[A-Za-z]|bis|ter|quater)?)\s+(.*)", address, re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).strip()  # (numéro, nom de la voie)
    return None, address.strip()  # Si pas de numéro, renvoyer None et l'adresse complète
 