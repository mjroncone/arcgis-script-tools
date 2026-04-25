#---------------------------------------------------------------------------------------
# Name:        test_gpkg
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
IMAGE_FOLDER_PATH = r"Z:\Documents\penn-state-gis\GEOG485\final-project\gpkg-images"

GDB_GPKG_PRIMARY_KEY = "gpkg_primary_key"

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

def get_gpkg_tables(db_cursor):
    query = db_cursor.execute("""
        SELECT name
        FROM sqlite_schema
        WHERE type='table' AND name NOT LIKE 'sqlite_%';
    """)
    return [table_name for (table_name, *_) in query.fetchall()]

def get_primary_key_columns(db_cursor, table_name):
    query = db_cursor.execute(f"""
        SELECT name
        FROM pragma_table_info("{table_name}")
        WHERE pk > 0
    """)
    return [column_name for (column_name, *_) in query.fetchall()]

def get_gpkg_related_tables_exts(db_cursor):
    query = db_cursor.execute("""
        SELECT table_name, column_name, extension_name, definition, scope
        FROM gpkg_extensions
        WHERE extension_name IN ('related_tables', 'gpkg_related_tables');
    """)
    return [GPKGExtensionRecord(row[0], row[1], row[2], row[3], row[4]) for row in query.fetchall()]

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

def get_gpkg_related_media(db_cursor, gpkg_relation, feature_id):
    query = db_cursor.execute(f"""
        SELECT media.'{gpkg_relation.related_primary_column}', data, content_type
        FROM "{gpkg_relation.related_table_name}" AS media
        JOIN "{gpkg_relation.mapping_table_name}" AS map ON map.related_id = media.'{gpkg_relation.related_primary_column}'
        JOIN "{gpkg_relation.base_table_name}" AS features ON map.base_id = features.'{gpkg_relation.base_primary_column}'
        WHERE features.'{gpkg_relation.base_primary_column}' = {feature_id};
    """)
    return [GPKGMediaRecord(identifier=row[0], data=row[1], content_type=row[2]) for row in query.fetchall()]

def log(msg):
    arcpy.AddMessage(msg)

def reportError(msg):
    arcpy.AddError(msg)

# What if I created a match table in the gdb right here? Or returned some information so that I could
# do it. I already have the table name, feature id, and the path to the image.
def extract_images(db_cursor, gpkg_table_name, gpkg_fc, image_folder_path):
    fc_relations = get_gpkg_media_relations(db_cursor, gpkg_table_name)
    if len(fc_relations) > 0:
        # Since the base is the same feature table, they should all have the same base primary column.
        primary_key_col = fc_relations[0].base_primary_column

        with arcpy.da.SearchCursor(gpkg_fc, '*') as cursor:
            primary_key_idx = cursor.fields.index(primary_key_col)

            for row in cursor:
                for fc_relation in fc_relations:
                    related_media = get_gpkg_related_media(db_cursor, fc_relation, row[primary_key_idx])
                    for media_record in related_media:
                        # TODO: new_fc_name?
                        file_ext = mimetypes.guess_extension(media_record.content_type)
                        file_name = f"{gpkg_table_name}_{row[primary_key_idx]}_{fc_relation.related_table_name}_{media_record.identifier}{file_ext}"
                        with open(f"{image_folder_path}/{file_name}", "wb") as file:
                            file.write(media_record.data)
            del cursor

def gpkg_table_name_to_gdb(table_name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

def main(gpkg_path=GPKG_PATH, output_gdb_path=OUTPUT_GDB_PATH, image_folder_path=IMAGE_FOLDER_PATH):
    try:
        arcpy.env.workspace = gpkg_path
        arcpy.env.overwriteOutput = True

        gpkg_desc = arcpy.da.Describe(gpkg_path)
        if not arcpy.Exists(gpkg_path) or gpkg_desc['dataType'] != 'Workspace' or not gpkg_desc['name'].endswith('.gpkg'):
            raise RuntimeError(f"Path does not point to a valid GeoPackage file: {gpkg_path}")


        if not arcpy.Exists(output_gdb_path):
            raise RuntimeError("Output geodatabase path does not exist.")

        gdb = output_gdb_path
        output_type = arcpy.Describe(output_gdb_path).dataType
        if output_type == 'Folder':
            new_gdb_name = gpkg_desc['name'].replace('.gpkg', '')
            log(f"Creating GDB: {new_gdb_name} at {output_gdb_path}")
            gdb = arcpy.management.CreateFileGDB(output_gdb_path, new_gdb_name)
        elif output_type == 'Workspace':
            log("Output is a geodatabase.")
        else:
            raise RuntimeError(f"Invalid option provided for output path. Must either be a folder or a geodatabase. Received: {output_gdb_path}")

        gpkg_fcs = arcpy.ListFeatureClasses()
        log(f"gpkg feature classes:\n{',\n '.join(gpkg_fcs)}\n")

        db = sqlite3.connect(gpkg_path)
        db_cursor = db.cursor()

        extensions = get_gpkg_related_tables_exts(db_cursor)
        if len(extensions) < 1:
            log("No related table extensions found.")
            return
        else:
            log(f"gpkg extensions from SQLite:\n {',\n '.join(map(str, extensions))}\n")

        log("Creating mapping table")

        # Ensure the base image_folder_path exists. I'll create temporary folders within
        # it to ensure there are no naming collisions.
        delete_image_folder_on_complete = False
        if not arcpy.Exists(image_folder_path):
            delete_image_folder_on_complete = True
            log(f"Creating images folder: {image_folder_path}")
            pathlib.Path(image_folder_path).mkdir(parents=False, exist_ok=False)
        tmp_image_folder_path = image_folder_path + "/" + str(uuid.uuid4())

        # TODO: should this be defined in the loop and have one for each feature class?
        image_match_field_name = "MatchID"
        image_match_file_field_name = "Filename"
        image_match_table_name = "image_matches"
        image_match_table = arcpy.management.CreateTable(gdb, image_match_table_name)
        arcpy.management.AddFields(
            image_match_table,
            [
                [image_match_field_name, "TEXT"],
                [image_match_file_field_name, "TEXT", image_match_file_field_name, 260]
            ]
        )

        for gpkg_fc in gpkg_fcs:
            fc_desc = arcpy.da.Describe(gpkg_fc)
            fc_table_name = fc_desc['extension']
            # file geodatabases don't allow special characters other than underscores
            new_fc_name = gpkg_table_name_to_gdb(fc_table_name)

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
{fc_table_name} -> gdb name: {new_fc_name}"
            )
            gdb_layer = f"{gdb}/{new_fc_name}"

            field_mappings = arcpy.FieldMappings()
            field_mappings.addTable(gpkg_fc)

            gpkg_id_field_map = arcpy.FieldMap()
            gpkg_id_field_map.addInputField(gpkg_fc, primary_key_column)
            gpkg_id_field = gpkg_id_field_map.outputField
            gpkg_id_field.name = GDB_GPKG_PRIMARY_KEY
            gpkg_id_field.alias = GDB_GPKG_PRIMARY_KEY
            gpkg_id_field_map.outputField = gpkg_id_field
            field_mappings.addFieldMap(gpkg_id_field_map)

            arcpy.conversion.ExportFeatures(gpkg_fc, gdb_layer, None, None, field_mappings)

            log(f"Adding global id to {new_fc_name} to support image attachments.")

            arcpy.management.AddGlobalIDs(gdb_layer)

            log(f"Enabling attachments on {new_fc_name}.")

            arcpy.management.EnableAttachments(gdb_layer)

            fc_relations = get_gpkg_media_relations(db_cursor, fc_table_name)
            if len(fc_relations) > 0:
                log("Extracting images")

                if not arcpy.Exists(tmp_image_folder_path):
                    log(f"Creating images folder: {image_folder_path}")
                    pathlib.Path(tmp_image_folder_path).mkdir(parents=False, exist_ok=False)

                with arcpy.da.SearchCursor(gdb_layer, [GDB_GPKG_PRIMARY_KEY, 'GlobalID']) as search_cursor:
                    with arcpy.da.InsertCursor(image_match_table, [image_match_field_name, image_match_file_field_name]) as insert_cursor:
                        for row in search_cursor:
                            feature_primary_key = row[0]
                            feature_global_id = row[1]

                            for fc_relation in fc_relations:
                                safe_relation_table_name = gpkg_table_name_to_gdb(fc_relation.related_table_name)

                                related_media = get_gpkg_related_media(db_cursor, fc_relation, feature_primary_key)

                                for media_record in related_media:
                                    file_ext = mimetypes.guess_extension(media_record.content_type)
                                    file_name = f"{new_fc_name}_{feature_primary_key}_{safe_relation_table_name}_{media_record.identifier}{file_ext}"
                                    file_path = f"{tmp_image_folder_path}/{file_name}"

                                    with open(file_path, "wb") as file:
                                        file.write(media_record.data)

                                    insert_cursor.insertRow([feature_global_id, file_name])

                    del insert_cursor

                del search_cursor

                log(f"Adding attachments to {new_fc_name}.")

                arcpy.management.AddAttachments(
                    gdb_layer,
                    'GlobalID',
                    image_match_table,
                    image_match_field_name,
                    image_match_file_field_name,
                    tmp_image_folder_path
                )

                # ArcGIS stores the images in its own format inside the GDB once attachments
                # have been created, so the copies saved to the folder are no longer needed.
                # Delete them after each layer to reduce memory usage when there are many
                # images across many layers.
                if arcpy.Exists(tmp_image_folder_path):
                    log(f"Removing temporary image folder: {tmp_image_folder_path}")
                    shutil.rmtree(pathlib.Path(tmp_image_folder_path))
            else:
                log(f"No related images found for {fc_table_name}")

        if delete_image_folder_on_complete and arcpy.Exists(image_folder_path):
            log(f"Removing image folder we created: {image_folder_path}")
            shutil.rmtree(pathlib.Path(image_folder_path))

        log("Complete.")
    except arcpy.ExecuteError as e:
        if "000464" in str(e):
            reportError("ERROR: Unable to get an exclusive lock on the data. Ensure that it is not open \
in any other programs, then try again.")
        else:
            reportError("Unknown execution error. Please restore your dataset to its original \
condition, close all other programs that may be accessing the data, and try again.")
            reportError(f"Received Error: {str(e)}")
            raise e # TODO: don't raise in final version
    except Exception as e:
        raise e # TODO: don't raise in final version
        reportError(f"Unknown Error: {str(e)}")
    finally:
        # Ensure any lockfiles are cleaned up since the script is complete.
        arcpy.management.ClearWorkspaceCache()

if __name__ == '__main__':
    main()
