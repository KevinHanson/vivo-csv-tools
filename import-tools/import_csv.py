#!/usr/bin/env python
import ConfigParser
import sys
from template_graph import TemplateGraph


def get_config(config, section, name):
    try:
        return config.get(section, name)
    except:
        return config.get("default", name)


def main():
    config = ConfigParser.ConfigParser()
    config.read("import_csv.cfg")
    cfg_section = sys.argv[1]

    g = TemplateGraph(config.get(cfg_section, "added_triples"))
    g.subtract(config.get(cfg_section, "base_triples"))
    print "New triples:"
    print super(TemplateGraph, g).serialize(format="nt")

    g.import_csv(get_config(config, cfg_section, "csv_file"),
                 get_config(config, cfg_section, "added_uri"),
                 get_config(config, cfg_section, "uri_prefix"),
                 get_config(config, cfg_section, "added_namespace"),
                 get_config(config, cfg_section, "destination_namespace"),
                 get_config(config, cfg_section, "uri_id_column"),
                 int(get_config(config, cfg_section, "start_id")),
                 get_config(config, cfg_section, "sparql_endpoint"),
                 get_config(config, cfg_section, "vivo_account"),
                 get_config(config, cfg_section, "vivo_pwd"))


if __name__ == "__main__":
    main()
