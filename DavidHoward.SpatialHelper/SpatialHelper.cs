using System;
using System.Collections;
using System.Collections.Generic;

using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;

using Microsoft.SqlServer.Types;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

using DotSpatial.Data;
using DotSpatial.Topology;
using GeoAPI.Geometries;

using NetTopologySuite.IO;
using NetTopologySuite.Geometries;



namespace DavidHoward.SpatialHelper
{
    /// <summary>
    /// Collection of spatial utilities compiled from various sources
    /// </summary>
    public class SpatialHelper
    {
        //========================================================
        #region SqlServerToDsFeatureSet
        static SpatialHelper()
        {
            GeoAPI.GeometryServiceProvider.Instance = NetTopologySuite.NtsGeometryServices.Instance;
        }

        /// <summary>
        /// Imports SQLServer spatial data from query into a DotSpatial featureset (not returning column names!!)
        /// </summary>
        /// <remarks>
        /// Code from FObermeier in http://dotspatial.codeplex.com/discussions/250928
        /// and https://dotspatial.codeplex.com/workitem/25687
        /// Requires DotSpatial.Data in calling module to accept return IFeatureSet type
        /// Usage example:  var fsPolygon = (FeatureSet)SqlServerToDsFeatureSet.LoadFeatureSet(ConnectionString, query);
        /// </remarks>
        /// 
        public static IFeatureSet SqlServerToDsFeatureSet(string connectionString, string sql)
        {
            IFeatureSet res = null;
            using (var cn = new SqlConnection(connectionString))
            {
                cn.Open();
                var cmd = new SqlCommand(sql, cn);
                using (var r = cmd.ExecuteReader())
                {
                    if (r.HasRows)
                    {
                        r.Read();
                        DataTable table;
                        Func<SqlDataReader, int, IBasicGeometry> gr;
                        int gIndex;
                        ColumMapper columMapper;

                        res = new FeatureSet(GetFeatureType(r, out table, out gr, out gIndex, out columMapper));

                        var values = new object[r.FieldCount];
                        table.BeginLoadData();
                        do
                        {
                            var g = gr(r, gIndex);
                            var num = r.GetValues(values);
                            var f = res.AddFeature(g);
                            var dr = table.LoadDataRow(columMapper.Map(values), true);
                            f.DataRow = dr;

                        } while (r.Read());

                        table.EndLoadData();
                    }
                }
                cmd.Dispose();
            }
            return res;
        }

        private class ColumMapper
        {
            private readonly Dictionary<int, int> _map = new Dictionary<int, int>();

            public object[] Map(object[] values)
            {
                var res = new object[_map.Count];
                foreach (var t in _map)
                    res[t.Value] = values[t.Key];

                return res;
            }

            public void AddMap(int o, int t)
            {
                _map.Add(o, t);
            }

        }

        private static class SqlServerReaderUtility
        {
            private static readonly MsSql2008GeometryReader GeometryReader = new MsSql2008GeometryReader();
            private static readonly MsSql2008GeographyReader GeographyReader = new MsSql2008GeographyReader();

            public static IBasicGeometry ReadGeometry(SqlDataReader reader, int index)
            {
                return GeometryReader.Read((SqlGeometry)reader.GetValue(index)).ToDotSpatial();
            }
            public static IBasicGeometry ReadGeography(SqlDataReader reader, int index)
            {
                return GeographyReader.Read((SqlGeography)reader.GetValue(index)).ToDotSpatial();
            }
        }

        private static FeatureType GetFeatureType(SqlDataReader r, out DataTable table, out Func<SqlDataReader, int, IBasicGeometry> gc, out int gIndex, out ColumMapper mapper)
        {
            table = new DataTable();
            mapper = new ColumMapper();
            gIndex = -1;
            gc = null;

            var res = FeatureType.Unspecified;

            for (var i = 0; i < r.FieldCount; i++)
            {
                var t = r.GetFieldType(i);
                if (t == null)
                    throw new InvalidOperationException("Could not get column type");

                if (t == typeof(SqlGeometry))
                {
                    gc = SqlServerReaderUtility.ReadGeometry;
                    gIndex = i;
                    res = gc(r, gIndex).FeatureType;
                }
                else if (t == typeof(SqlGeography))
                {
                    gc = SqlServerReaderUtility.ReadGeography;
                    gIndex = i;
                    res = gc(r, gIndex).FeatureType;
                }
                else
                {
                    table.Columns.Add(r.GetName(i), t);
                    mapper.AddMap(i, table.Columns.Count - 1);
                }
            }

            //if (gIndex == -1)
            //    throw new InvalidOperationException("No geometry column found");

            return res;
        }

        #endregion SqlServerToDsFeatureSet
        //========================================================

        /// <summary>
        /// Shapefile features to data table (to simplify checking contents)
        /// </summary>
        /// <remarks>
        /// Uses DotSpatial Featureset to get shapefile features 
        /// If srid > 0 then returns SqlGeography type; otherwise SqlGeometry. TODO: use shapefile projection info to get SRID
        /// Ring direction may need to be reoriented for geography type. TODO: auto-compute if reorientation needed
        /// </remarks>
        public static DataTable ShapeToDataTable(string shapeFile, int srid, string spatialColumnName, bool reorientRing, out Exception exception)
        {
            var dt = new DataTable();
            exception = null;
            try
            {
                var fs = (FeatureSet)FeatureSet.Open(shapeFile);

                dt = fs.DataTable;                //copy shapefile datatable

                //add spatial column
                if (srid > 0)
                    dt.Columns.Add(spatialColumnName, typeof(SqlGeography));
                else
                    dt.Columns.Add(spatialColumnName, typeof(SqlGeometry));

                var i = 0;
                foreach (var f in fs.Features)                //copy shapefile spatial field into datatable
                {
                    var geomString = f.BasicGeometry.ToString();

                    if (srid > 0)
                    {
                        var myGeog = SqlGeography.STGeomFromText(new SqlChars(geomString), 4283);
                        if (reorientRing)
                            myGeog = myGeog.ReorientObject();

                        dt.Rows[i][spatialColumnName] = myGeog;
                    }
                    else
                    {
                        var myGeom = SqlGeometry.STGeomFromText(new SqlChars(geomString), srid);
                        dt.Rows[i][spatialColumnName] = myGeom;
                    }
                    i++;
                }
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return dt;
        }

        /// <summary>
        /// DotSpatial features to data table (to simplify checking contents)
        /// </summary>
        public static DataTable DsFeatureSetToDataTable(FeatureSet fs)
        {
            var dt = new DataTable();

            var numCols = fs.Features[0].DataRow.ItemArray.Count();

            for (var i = 0; i < numCols; i++)
            {
                dt.Columns.Add();
            }

            foreach (var f in fs.Features)
            {
                dt.Rows.Add(f.DataRow.ItemArray);
            }

            return dt;
        }

        /// <summary>
        /// Copy data from DataTable to SQL Server Database using SQLServer Management Objects
        /// isNewTable = true: will (drop and) create new SQL Server table
        /// </summary>
        ///         
        /// <remarks>
        /// Code largely from:
        ///   www.codeproject.com/Articles/17169/Copy-Data-from-a-DataTable-to-a-SQLServer-Database
        ///   http://technico.qnownow.com/table-operations-using-smo/
        /// See readme.txt  for references required
        /// </remarks>
        public static void DataTableToSqlTable(DataTable dataTable, string connectionString, string databaseName,  string sqlTablename, bool isNewTable, out Exception exception)
        {
            exception = null;
            try
            {
                var connection = new SqlConnection(connectionString);
                var server = new Server(new ServerConnection(connection));
                var db = server.Databases[databaseName];            //set target db
                var sqlTable = new Table(db, sqlTablename);

                //SMO Column object referring to destination table.
                var tempC = new Column();

                //Add the column names and types from the datatable into the new table
                //Using the columns name and type property
                foreach (DataColumn dc in dataTable.Columns)                    //Create columns from datatable column schema
                {
                    tempC = new Column(sqlTable, dc.ColumnName);
                    tempC.DataType = GetDataType(dc.DataType.ToString());

                    sqlTable.Columns.Add(tempC);
                }

                var table = db.Tables[sqlTablename];                //try getting table reference from database               
                var tableExists = (table != null);

                if (tableExists && isNewTable)        //table exists and new table wanted... drop and recreate
                {
                    table.Drop();
                    sqlTable.Create();
                }
                else if (!tableExists && !isNewTable)  //doesn't exist and no new table specified, so create it anyway
                {
                    sqlTable.Create();
                }
                // else table exists already and new table not required (i.e. copy data to existing table)

                using (var bulkCopy = new SqlBulkCopy(connectionString))
                {
                    bulkCopy.DestinationTableName = sqlTable.Name;
                    bulkCopy.WriteToServer(dataTable);
                }
                connection.Close();
            }
            catch (Exception ex)
            {
                exception = ex;
            }
        }

        private static DataType GetDataType(string dataType)
        {
            DataType DTTemp = null;

            switch (dataType)
            {
                case ("System.Single"):
                    DTTemp = DataType.Float;
                    break;

                case ("System.Decimal"):
                    DTTemp = DataType.Decimal(2, 18);
                    break;
                case ("System.String"):
                    DTTemp = DataType.VarChar(50);
                    break;
                case ("System.DateTime"):
                    DTTemp = DataType.DateTime;
                    break;
                case ("System.Int32"):
                    DTTemp = DataType.Int;
                    break;

                case ("Microsoft.SqlServer.Types.SqlGeography"):
                    DTTemp = DataType.Geography;
                    break;
                case ("Microsoft.SqlServer.Types.SqlGeometry"):
                    DTTemp = DataType.Geometry;
                    break;

            }
            return DTTemp;
        }

        /// <summary>
        /// Gets basic shapefile header info into a hashtable
        /// </summary>
        /// <remarks>
        /// code based on  //http://dominoc925.blogspot.com.au/2013/04/using-nettopologysuite-to-read-and.html
        /// using NetTopologySuite.IO;
        /// </remarks>
        /// <param name="shapefile">path to shapefile</param>
        /// <returns>hshtable with header key/pair values</returns>
        public static Hashtable GetShapefileHeaderInfo(string shapefile)
        {
            var hashTable = new Hashtable();

            var factory = new NetTopologySuite.Geometries.GeometryFactory();
            var shapeFileDataReader = new ShapefileDataReader(shapefile, factory);
            var shpHeader = shapeFileDataReader.ShapeHeader;
            var dbHeader = shapeFileDataReader.DbaseHeader;

            hashTable.Add("Bounds", shpHeader.Bounds);
            hashTable.Add("ShapeType", shpHeader.ShapeType);
            hashTable.Add("FileLength", shpHeader.FileLength);

            hashTable.Add("NumFields", dbHeader.NumFields);
            hashTable.Add("NumRecords", dbHeader.NumRecords);
            hashTable.Add("LastUpdate", dbHeader.LastUpdateDate);

            return hashTable;
        }

    }

}
