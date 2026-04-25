#---------------------------------------------------------------------------------------
# Name:        attach_gpkg_related_media
# Purpose:
#
# Author:      mikeroncone
#
# Created:     04/2026
#---------------------------------------------------------------------------------------

import arcpy
import sqlite3
import re
import pathlib
import shutil
import mimetypes
import uuid

GPKG_PATH = r"Z:\Documents\penn-state-gis\GEOG485\final-project\related-tables-gpkg.gpkg"
OUTPUT_GDB_PATH = r"Z:\Documents\penn-state-gis\GEOG485\final-project/"
TMP_DIR_PATH = r"Z:\Documents\penn-state-gis\GEOG485\final-project\gpkg-images"

GDB_GPKG_PRIMARY_KEY = "gpkg_primary_key"
GLOBAL_ID_FIELD = "GlobalID"

# The following GPKG{X} classes are data access objects for GeoPackage tables described
# in the specification, currently version 1.4.0: https://www.geopackage.org/spec140/index.html

class GPKGExtensionRecord:
    def __init__(self, table_name, column_name, extension_name, definition, scope):
        self.table_name = table_name
        self.column_name = column_name
        self.extension_name = extension_name
        self.definition = definition
        self.scope = scope

    def __repr__(self):
        return f"GPKGExtensionRecord(table_name={self.table_name}, column_name={self.column_name}, \
extension_name={self.extension_name}, definition={self.definition}, scope={self.scope})"

class GPKGRelationRecord:
    def __init__(
        self,
        identifier,
        base_table_name,
        base_primary_column,
        related_table_name,
        related_primary_column,
        relation_name,
        mapping_table_name
    ):
        self.identifier = identifier
        self.base_table_name = base_table_name
        self.base_primary_column = base_primary_column
        self.related_table_name = related_table_name
        self.related_primary_column = related_primary_column
        self.relation_name = relation_name
        self.mapping_table_name = mapping_table_name

    def __repr__(self):
        return f"GPKGRelationRecord(identifier={self.identifier}, base_table_name={self.base_table_name}, \
base_primary_column={self.base_primary_column}, related_table_name={self.related_table_name}, \
related_primary_column={self.related_primary_column}, relation_name={self.relation_name}, \
mapping_table_name={self.mapping_table_name})"

class GPKGRelationMapRecord:
    def __init__(
        self,
        base_id,
        related_id
    ):
        self.base_id = base_id
        self.related_id = related_id

    def __repr__(self):
        return f"GPKGRelationMapRecord(base_id={self.base_id}, related_id={self.related_id})"

class GPKGMediaRecord:
    def __init__(
        self,
        identifier,
        data,
        content_type
    ):
        self.identifier = identifier
        self.data = data
        self.content_type = content_type

    def __repr__(self):
        return f"GPKGRelationMapRecord(identifier={self.identifier}, data={self.data}, \
content_type={self.content_type})"

def log(msg):
    arcpy.AddMessage(msg)

# Find all columns in a table that are marked as primary keys
# This is useful if you need to query a table by primary key without
# knowing it beforehand.
def get_primary_key_columns(db_cursor, table_name):
    query = db_cursor.execute(f"""
        SELECT name
        FROM pragma_table_info("{table_name}")
        WHERE pk > 0
    """)
    return [column_name for (column_name, *_) in query.fetchall()]

# Find all gpkg_extensions records matching the gpkg_related_tables extension
# described here: https://docs.ogc.org/is/18-000/18-000.html
def get_gpkg_related_tables_exts(db_cursor):
    query = db_cursor.execute("""
        SELECT table_name, column_name, extension_name, definition, scope
        FROM gpkg_extensions
        WHERE extension_name IN ('related_tables', 'gpkg_related_tables');
    """)
    return [GPKGExtensionRecord(row[0], row[1], row[2], row[3], row[4]) for row in query.fetchall()]

# Find all relations that adhere to the media conformance class: https://docs.ogc.org/is/18-000/18-000.html#_media
def get_gpkg_media_relations(db_cursor,  base_table_name):
    conditional_clause = "relation_name='media'"

    if base_table_name is not None:
        conditional_clause += f" AND base_table_name = '{base_table_name}'"

    query = db_cursor.execute(f"""
        SELECT id, base_table_name, base_primary_column, related_table_name,
            related_primary_column, relation_name, mapping_table_name
        FROM gpkgext_relations
        WHERE {conditional_clause};
    """)
    return [GPKGRelationRecord(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in query.fetchall()]

# Given a specific gpkgext_relation record, find all media records in the tables described by it.
def get_gpkg_related_media(db_cursor, gpkg_relation, feature_id):
    query = db_cursor.execute(f"""
        SELECT media.'{gpkg_relation.related_primary_column}', data, content_type
        FROM "{gpkg_relation.related_table_name}" AS media
        JOIN "{gpkg_relation.mapping_table_name}" AS map ON map.related_id = media.'{gpkg_relation.related_primary_column}'
        JOIN "{gpkg_relation.base_table_name}" AS features ON map.base_id = features.'{gpkg_relation.base_primary_column}'
        WHERE features.'{gpkg_relation.base_primary_column}' = {feature_id};
    """)
    return [GPKGMediaRecord(identifier=row[0], data=row[1], content_type=row[2]) for row in query.fetchall()]

# GeoPackages allow more symbols in the table names than Geodatabases do, which only allow
# underscores. Convert all other symbols to underscores.
def gpkg_table_name_to_gdb(table_name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

def copy_fc_to_gdb(gpkg_fc, pk_column, gdb, table_name):
    gdb_layer = f"{gdb}/{table_name}"

    # ExportFeatures and other geoprocessing tools typically strip out the auto-incrementing
    # primary key for the table during the conversion. We need that key to match
    # the media records to the correct feature, so add a specific mapping for it to
    # a new column that we can reference later.
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(gpkg_fc)
    gpkg_id_field_map = arcpy.FieldMap()
    gpkg_id_field_map.addInputField(gpkg_fc, pk_column)
    gpkg_id_field = gpkg_id_field_map.outputField
    gpkg_id_field.name = GDB_GPKG_PRIMARY_KEY
    gpkg_id_field.alias = GDB_GPKG_PRIMARY_KEY
    gpkg_id_field_map.outputField = gpkg_id_field
    field_mappings.addFieldMap(gpkg_id_field_map)

    arcpy.conversion.ExportFeatures(gpkg_fc, gdb_layer, None, None, field_mappings)

    return gdb_layer

# Given a set of geopackage media relation records for a geodatbase layer, extract the images from the geopackage
# and attach them to the appropriate records in the geodatabase.
def attach_related_images(gpkg_db_cursor, fc_relations, gdb_layer, img_dir, img_match_table, match_field, file_field):
    gdb_table_name = arcpy.Describe(gdb_layer).name

    if not arcpy.Exists(img_dir):
        log(f"Creating images folder: {img_dir}")
        pathlib.Path(img_dir).mkdir(parents=False, exist_ok=False)

    try:
        with arcpy.da.SearchCursor(gdb_layer, [GDB_GPKG_PRIMARY_KEY, GLOBAL_ID_FIELD]) as search_cursor:
            with arcpy.da.InsertCursor(img_match_table, [match_field, file_field]) as insert_cursor:
                for row in search_cursor:
                    feature_primary_key = row[0]
                    feature_global_id = row[1]

                    for fc_relation in fc_relations:
                        related_media = get_gpkg_related_media(gpkg_db_cursor, fc_relation, feature_primary_key)
                        safe_relation_table_name = gpkg_table_name_to_gdb(fc_relation.related_table_name)

                        for media_record in related_media:
                            file_ext = mimetypes.guess_extension(media_record.content_type)
                            file_name = f"{gdb_table_name}_{feature_primary_key}_{safe_relation_table_name}_{media_record.identifier}{file_ext}"
                            file_path = f"{img_dir}/{file_name}"

                            with open(file_path, "wb") as file:
                                file.write(media_record.data)

                            insert_cursor.insertRow([feature_global_id, file_name])

            del insert_cursor
        del search_cursor

        arcpy.management.AddAttachments(
            gdb_layer,
            GLOBAL_ID_FIELD,
            img_match_table,
            match_field,
            file_field,
            img_dir
        )
    except Exception:
        # re-raise any exceptions. This block is only here to ensure the 'finally' statement runs.
        raise
    finally:
        # ArcGIS stores the images in its own format inside the GDB once attachments
        # have been created, so the copies saved to the folder are no longer needed.
        # Delete them after each layer to reduce memory usage when there are many
        # images across many layers.
        if arcpy.Exists(img_dir):
            log(f"Removing temporary image folder: {img_dir}")
            shutil.rmtree(pathlib.Path(img_dir))

def convert_gpkg_to_gdb(gpkg, gdb, tmp_dir):
    gpkg_fcs = arcpy.ListFeatureClasses()
    db = sqlite3.connect(gpkg)
    db_cursor = db.cursor()

    tmp_img_folder_path = tmp_dir + "/" + str(uuid.uuid4())
    # TODO: should this be defined in the loop and have one for each feature class?
    img_match_field_name = "MatchID"
    img_match_file_field_name = "Filename"
    img_match_table_name = "image_matches"

    img_match_table = None

    extensions = get_gpkg_related_tables_exts(db_cursor)
    if len(extensions) > 0:
        log("Creating the image mapping table")

        # I can't use arcpy.management.GenerateAttachmentMatchTable because there appears to be a
        # bug where it creates the match field as a BigInteger type, and the arcpy.management.AddAttachments
        # tool doesn't accept that as a valid type for matching. So instead I create a custom
        # mapping table and utiliz the GlobalID assigned to features later on in this function.
        img_match_table = arcpy.management.CreateTable(gdb, img_match_table_name)
        arcpy.management.AddFields(
            img_match_table,
            [
                [img_match_field_name, "TEXT"],
                [img_match_file_field_name, "TEXT", img_match_file_field_name, 260]
            ]
        )
    else:
        log("No related table extensions found.")


    for gpkg_fc in gpkg_fcs:
        fc_desc = arcpy.da.Describe(gpkg_fc)
        fc_table_name = fc_desc['extension']
        # File Geodatabases have stricter character requirements
        gdb_fc_name = gpkg_table_name_to_gdb(fc_table_name)

        primary_key_column = None
        primary_key_candidates = get_primary_key_columns(db_cursor, fc_table_name)
        if len(primary_key_candidates) > 1:
            raise ValueError(f"Table {fc_table_name} has a compound primary key, which is not yet supported.")
        elif len(primary_key_candidates) < 1:
            raise ValueError(f"Unable to find a primary key for {fc_table_name}, which is required for related media extraction.")
        else:
            primary_key_column = primary_key_candidates[0]


        log(
            f"Copying GeoPackage feature class to the Geodatabase gpkg name: \
{fc_table_name} -> gdb name: {gdb_fc_name}"
        )

        gdb_layer = copy_fc_to_gdb(gpkg_fc, primary_key_column, gdb, gdb_fc_name)

        if img_match_table is not None:
            log(f"Adding global id to {gdb_fc_name} to support image attachments.")

            arcpy.management.AddGlobalIDs(gdb_layer)

            log(f"Enabling attachments on {gdb_fc_name}.")

            arcpy.management.EnableAttachments(gdb_layer)

            fc_relations = get_gpkg_media_relations(db_cursor, fc_table_name)
            if len(fc_relations) > 0:
                log(f"Attaching images for {len(fc_relations)} relations to {gdb_layer}")
                attach_related_images(db_cursor, fc_relations, gdb_layer, tmp_img_folder_path, img_match_table, img_match_field_name, img_match_file_field_name)
            else:
                log(f"No related media tables found for {fc_table_name}")

    if img_match_table is not None:
        log("Deleting the image match table now that it's no longer needed.")
        arcpy.management.Delete(img_match_table)

def main(gpkg_path=GPKG_PATH, output_gdb_path=OUTPUT_GDB_PATH, tmp_dir=TMP_DIR_PATH):
    delete_gdb_on_failure = False
    failed_to_complete = False
    delete_tmp_folder_on_complete = False

    try:
        arcpy.env.workspace = gpkg_path
        arcpy.env.overwriteOutput = True

        gpkg_desc = arcpy.da.Describe(gpkg_path)
        if not arcpy.Exists(gpkg_path) or gpkg_desc['dataType'] != 'Workspace' or not gpkg_desc['name'].endswith('.gpkg'):
            raise RuntimeError(f"Path does not point to a valid GeoPackage file: {gpkg_path}")


        if not arcpy.Exists(output_gdb_path):
            raise RuntimeError("Output geodatabase path does not exist.")

        gdb = None
        output_type = arcpy.Describe(output_gdb_path).dataType
        if output_type == 'Workspace':

            # TODO: is there a more detailed check I can do? Other things qualify as
            # workspaces, too. If provided via script tool, I can do validations on that input,
            # but if running as a standalone script this could cause unhandled errors.
            log("Output is a geodatabase.")
            gdb = output_gdb_path

        elif output_type == 'Folder':

            new_gdb_name = gpkg_desc['name'].replace('.gpkg', '')
            log(f"Creating GDB: {new_gdb_name} at {output_gdb_path}")
            new_file_path = f"{output_gdb_path}/{new_gdb_name}.gdb"

            if arcpy.Exists(new_file_path):
                # Raising an error here because it's coincidental, and may indicate that
                # some data is about to be overwritten unintentionally if we proceed.
                raise RuntimeError(f"A geodatabase with the name {new_gdb_name} already exists \
please rename either the GeoPackage or the Geodatabase and try again.")
            else:
                gdb = arcpy.management.CreateFileGDB(output_gdb_path, new_gdb_name)
                delete_gdb_on_failure = True

        else:
            raise RuntimeError(f"Invalid option provided for output path. Must either be a folder or a geodatabase. Received: {output_gdb_path}")

        # Ensure the base tmp_dir exists. I'll create temporary folders within
        # it to ensure there are no naming collisions.
        if not arcpy.Exists(tmp_dir):
            delete_tmp_folder_on_complete = True
            log(f"Creating temporary working directory for images: {tmp_dir}")
            pathlib.Path(tmp_dir).mkdir(parents=False, exist_ok=False)

        convert_gpkg_to_gdb(gpkg_path, gdb, tmp_dir)

        log("Complete.")
    except arcpy.ExecuteError as e:
        failed_to_complete = True
        error_msg = str(e)
        if "000464" in error_msg or "000210" in error_msg:

            arcpy.AddError("ERROR: Unable to get an exclusive lock on the data. Ensure that it is not open \
in any other programs, then try again.")
        else:
            arcpy.AddError("Unhandled execution error. Please restore your dataset to its original \
condition, close all other programs that may be accessing the data, and try again.")
            arcpy.AddError(f"Received: {error_msg}")

    except Exception as e:
        failed_to_complete = True

        arcpy.AddError(f"Unknown Error: {str(e)}")

    finally:
        # Ensure any lockfiles are cleaned up since the script is complete.
        arcpy.management.ClearWorkspaceCache()

        if delete_gdb_on_failure and failed_to_complete and arcpy.Exists(gdb):
            log(f"Removing incomplete gdb we created: {gdb}")
            arcpy.management.Delete(gdb)

        if delete_tmp_folder_on_complete and arcpy.Exists(tmp_dir):
            log(f"Removing temp folder we created: {tmp_dir}")
            shutil.rmtree(pathlib.Path(tmp_dir))


if __name__ == '__main__':
    main()
