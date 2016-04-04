# -*- coding: utf-8 -*-
"""
Command line application to export VIVO data to CSV.
Usage: export_csv.py <config section> <output file name>

.. moduleauthor:: rdj
"""

import ConfigParser
import sys
from template_graph import TemplateGraph


def main():
#    config = ConfigParser.ConfigParser()
#    config.read("test.cfg")
    cfg_section = sys.argv[1]

    g = TemplateGraph("test.cfg", cfg_section)
    print "Exported %d rows." % g.export_csv(sys.argv[2])


if __name__ == "__main__":
    main()
