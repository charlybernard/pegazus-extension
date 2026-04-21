import os
import yaml
from dataclasses import dataclass
from rdflib import URIRef

@dataclass
class Folders:
    tmp: str
    data: str

@dataclass
class Files:
    ontology: str
    ruleset: str
    local_config: str
    labels: str
    comparisons: str

@dataclass
class GraphDB:
    base_url: str
    repo_name: str
    repo_url: URIRef
    lang: str
    facts_label: str

@dataclass
class NamedGraphs:
    ontology: str
    facts: str
    inter_sources: str
    comparisons: str
    labels: str
    temporary: str
    metadata: str

class ProjectConfig:
    """
    Project configuration class that organizes paths and settings into structured categories.
    """
    def __init__(self, user_params: dict, config_path: str):
        # Load the technical YAML config
        with open(config_path, "r") as f:
            tech = yaml.safe_load(f)

        self.tech = tech
        self.user_params = user_params

        self.set_folders()
        self.set_files()
        self.set_graphdb()
        self.set_named_graphs()
        self.set_comparison_settings()

    def set_folders(self):
        tmp_dir = os.path.abspath(self.tech['paths']['tmp_folder'])
        data_dir = os.path.abspath(self.user_params['data_folder_name'])
        self.folders = Folders(tmp=tmp_dir, data=data_dir)

    def set_files(self):
        tmp_dir = self.folders.tmp
        self.files = Files(
            ontology=os.path.abspath(self.user_params['ont_file_name']),
            ruleset=os.path.abspath(self.user_params['ruleset_file_name']),
            local_config=os.path.join(tmp_dir, self.tech['paths']['local_config_file']),
            labels=os.path.join(tmp_dir, self.tech['paths']['pref_hidden_labels_file']),
            comparisons=os.path.join(tmp_dir, self.tech['paths']['comparisons_file'])
        )

    def set_graphdb(self):
        self.graphdb = GraphDB(
            base_url=self.user_params['str_graphdb_url'],
            repo_name=self.user_params['repository_name'],
            repo_url=URIRef(self.user_params['str_graphdb_url']),
            lang=self.tech['labels'].get('lang', 'en'),
            facts_label=self.tech['labels'].get('facts_label', 'Facts')
        )

    def set_named_graphs(self):
        self.named_graphs = NamedGraphs(
            ontology=self.tech['named_graphs']['ontology'] or "ontology",
            facts=self.tech['named_graphs']['facts'] or "facts",
            inter_sources=self.tech['named_graphs']['inter_sources'] or "inter_sources",
            comparisons=self.tech['named_graphs']['comparisons'] or "comparisons",
            labels=self.tech['named_graphs']['labels'] or "labels",
            temporary=self.tech['named_graphs']['temporary'] or "temporary",
            metadata=self.tech['named_graphs']['metadata'] or "metadata"
        )

    def set_comparison_settings(self):
        self.comparison_settings = self.tech['comparison_settings']
        self.comparison_settings['geom_crs_uri'] = URIRef(self.tech['comparison_settings']['geom_crs_uri'])

    def __repr__(self):
        return f"ProjectConfig(repository='{self.graphdb.repo_name}')"