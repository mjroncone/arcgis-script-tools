# ArcGIS Script Tools

This repository holds some scripts that can help make handling data in ArcGIS easier.

### Viewing GeoPackage related image files in ArcGIS Pro
The first script, `attach-gpkg-related-media.py`, parses a GeoPackage file with the Related Tables extension for attaching media files to features. It creates a File Geodatabase from the GeoPackage, and then attaches any found media files to their related features using arcpy. These images can then be viewed in feature pop-ups or in the Attributes pane for selected features in ArcGIS Pro. 

To run the script from the command line, run `python attach-gpkg-related-media.py --help` to learn more about the arguments you need to supply. Note that you'll need to have an environment set up with access to arcpy.

To run the script from the ArcGIS User Interface, the python script has been embedded in the `gpkg-related-tables-tools.atbx` toolbox file. Download the toolbox file, add it to your ArcGIS Pro project, double click on it and follow the prompts.
