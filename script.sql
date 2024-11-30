                    re_query = """
                        SELECT MAX(num_order_in_file)
                        FROM peers 
                        WHERE file_name = '%s
                    """
                    param = (file_name,)

                    mycursor.execute(re_query, param)
                    number = mycursor.fetchone()
                    

                    query = f"""
                        SELECT peers_ip, peers_port, peers_hostname, file_name, file_size, piece_hash, piece_size, num_order_in_file
                        FROM (WITH NumberedRows AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY num_order_in_file ORDER BY RAND()) AS rn
                                FROM (
                                    SELECT * FROM peers
                                    WHERE file_name = %s
                                ) AS table1
                                WHERE num_order_in_file BETWEEN 1 AND ({number})
                            )
                            SELECT *
                            FROM NumberedRows
                            WHERE rn = 1) AS table2
                    """
                    params = (file_name, *number)

                mycursor.execute(query, params)
                results = mycursor.fetchall()