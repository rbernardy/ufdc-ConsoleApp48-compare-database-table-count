using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using rrb.ufdc.utility;

namespace ConsoleApp48
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string dbname = args[0];

            SqlConnection conn = general.getMSSQLconnection("ncf",null);
            SqlConnection connrrb;

            try
            {
                connrrb = general.getMSSQLconnection("mssql.richardbernardy.com", null);
            }
            catch (Exception ex)
            {
                Console.WriteLine("exception trying to connect to mssql.richardbernardy.com.");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return;
            }

            SqlCommand cmd = conn.CreateCommand();
            cmd.Connection = conn;

            string sql = @"SELECT
      QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [TableName]
      , SUM(sPTN.Rows) AS [RowCount]
FROM 
      sys.objects AS sOBJ
      INNER JOIN sys.partitions AS sPTN
            ON sOBJ.object_id = sPTN.object_id
WHERE
      sOBJ.type = 'U'
      AND sOBJ.is_ms_shipped = 0x0
      AND index_id < 2 -- 0:Heap, 1:Clustered
GROUP BY 
      sOBJ.schema_id
      , sOBJ.name
ORDER BY [RowCount] desc";

            cmd.CommandText = sql;
            SqlDataReader dr = cmd.ExecuteReader();

            SqlCommand cmd2 = connrrb.CreateCommand();
            cmd2.Connection = connrrb;
            SqlDataReader dr2;
            string insert;

            if (dr.HasRows)
            {
                Console.WriteLine("Has rows.");
                int idx = 0;

                while (dr.Read())
                {
                    Console.WriteLine(idx + ". " + dr[0].ToString() + "=" + dr[1].ToString());
                    insert = "insert into row_count ([server],[database],tablename,[rowcount]) values ('uf','" + dbname + "','" + dr[0] + "'," + dr[1] + ")";
                    cmd2.CommandText = insert;
                    int ar = cmd2.ExecuteNonQuery();
                    Console.WriteLine("ar=[" + ar + "] [" + insert + "].");
                }
            }

            conn.Close();
            connrrb.Close();
        }
    }
}