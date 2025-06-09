import psycopg2
import psycopg2.sql
import traceback
import io

ratingstablename = 'ratings'
INPUT_FILE_PATH = 'ratings.dat'

# Kết nối đến cơ sở dữ liệu PostgreSQL
def getopenconnection(user='postgres',password='1234',dbname='dds_assgn1'): #Thay đổi user, password nếu cần thiết
    return psycopg2.connect("dbname='"+dbname+"' user='"+user+"' host='localhost' password='"+password+"'")

# Lưu dữ liệu vào bảng ratings
def loadratings(ratingstablename,ratingsfilepath,openconnection):
    current=openconnection.cursor()

    # Xóa bảng ratings nếu đã tồn tại
    droptable="drop table if exists "+ratingstablename + ";"
    current.execute(droptable)

    # Tạo bảng ratings nếu chưa tồn tại
    createtable="create table if not exists "+ratingstablename+" (userid INT, movieid INT, rating float);"
    current.execute(createtable)

    # Tạo bộ nhớ tạm để lưu dữ liệu từ file
    buffer=io.StringIO()

    # Giới hạn số lượng dòng đọc từ file
    max_rows = 10000054 # =ACTUAL_ROWS_IN_INPUT_FILE
    current_rows = 0

    # Đọc dữ liệu từ file và ghi vào bộ nhớ
    with open(ratingsfilepath, 'r', encoding='utf-8') as infile:
        for line in infile:
            if current_rows >= max_rows: # Dừng khi đã đọc đủ số dòng
                break
            parts = line.strip().split('::')
            if len(parts) == 4:
                buffer.write(f"{parts[0]},{parts[1]},{parts[2]}\n")
                current_rows += 1
    buffer.seek(0)  # Quay lại đầu bộ nhớ để COPY đọc

    # Dùng COPY để tải dữ liệu vào bảng
    current.copy_expert(f"""
        copy {ratingstablename} (userid, movieid, rating)
        from stdin with (format csv)
    """, buffer)
    
    # Lưu các thay đổi vào cơ sở dữ liệu
    openconnection.commit()
    current.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    print(f"Bắt đầu rangepartition bảng '{ratingstablename}' thành {numberofpartitions} phân mảnh.")

    # Kiểm tra dữ liệu đầu vào
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        print("Số lượng phân mảnh phải là một số nguyên dương.")
        return
    current = openconnection.cursor()
    try:
        # Schema của bảng phân mảnh
        partition_table_schema_sql = "UserID INT, MovieID INT, Rating FLOAT"
        select_columns_sql = "UserID, MovieID, Rating"

        # Tính toán kích thước của mỗi phân vùng
        range_size = 5.0 / numberofpartitions

        # Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            # Tạo tên bảng phân mảnh
            RANGE_TABLE_PREFIX = 'range_part'
            partition_table_name = f"{RANGE_TABLE_PREFIX}{i}"
            partition_table_identifier = psycopg2.sql.Identifier(partition_table_name)
            ratings_table_identifier = psycopg2.sql.Identifier(ratingstablename)

            # Xóa bảng nếu đã tồn tại
            current.execute(psycopg2.sql.SQL("DROP TABLE IF EXISTS {};").format(
                partition_table_identifier
            ))

            # Tính toán giá trị bắt đầu và kết thúc cho phân mảnh
            start_value = i * range_size
            end_value = (i + 1) * range_size

            # Tạo bảng phân mảnh
            current.execute(psycopg2.sql.SQL("CREATE TABLE {} ({});").format(
                partition_table_identifier,
                psycopg2.sql.SQL(partition_table_schema_sql)
            ))

            # Chèn dữ liệu vào bảng phân vùng
            if i == 0:
                # Bao gồm cả giá trị = start_value
                current.execute(psycopg2.sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT {} FROM {}
                    WHERE rating >= %s AND rating <= %s;
                """).format(
                    partition_table_identifier,
                    psycopg2.sql.SQL(select_columns_sql),
                    psycopg2.sql.SQL(select_columns_sql),
                    ratings_table_identifier
                ), (start_value, end_value))
            else:
                current.execute(psycopg2.sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT {} FROM {}
                    WHERE rating > %s AND rating <= %s;
                """).format(
                    partition_table_identifier,
                    psycopg2.sql.SQL(select_columns_sql),
                    psycopg2.sql.SQL(select_columns_sql),
                    ratings_table_identifier
                ), (start_value, end_value))
        openconnection.commit()
        print("Phân mảnh thành công!")
    except psycopg2.Error as e:
        print(f"Lỗi PostgreSQL trong rangepartition: {e}")
        openconnection.rollback()
        raise
    except Exception as ex:
        print(f"Lỗi không xác định trong rangepartition: {ex}")
        traceback.print_exc()
        openconnection.rollback()
        raise
    finally:
        current.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    print(f"Bắt đầu chèn bộ dữ liệu vào bảng '{ratingstablename}' với Userid={userid}, MovieID={itemid}, Rating={rating}.")
    validaterating = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    # Kiểm tra dữ liệu đầu vào
    if rating not in validaterating:
        print("Rating không hợp lệ. Nó phải nằm trong các giá trị từ 0.0 đến 5.0, cách nhau 0.5.")
        return
    current = openconnection.cursor()
    try:
        # Schema của bảng phân mảnh
        rating_table_identifier = psycopg2.sql.Identifier(ratingstablename)
        select_columns_sql = psycopg2.sql.SQL("userid, movieid, rating")

        # Dùng lệnh INSERT để thêm dữ liệu vào bảng ratings
        current.execute(
            psycopg2.sql.SQL("INSERT INTO {} ({}) VALUES (%s, %s, %s)").format(
                rating_table_identifier,
                select_columns_sql
            ),
            (userid, itemid, rating)
        )

        # Tiền tố tên bảng phân vùng
        RANGE_TABLE_PREFIX = 'range_part'

        # Truy vấn tính số lượng phân mảnh hiện có (sử dụng SQL composition)
        current.execute(
            psycopg2.sql.SQL("SELECT count(*) FROM pg_stat_user_tables WHERE relname LIKE {}").format(
                psycopg2.sql.Literal(f"{RANGE_TABLE_PREFIX}%")
            )
        )

        # Lấy số lượng phân mảnh
        numberofpartitions = current.fetchone()[0]

        # Kiểm tra số lượng phân mảnh
        if numberofpartitions <= 0:
            raise ValueError("Không có phân mảnh nào được tạo. Vui lòng tạo phân mảnh trước khi chèn dữ liệu.")

        # Tính khoảng cách của mỗi phân mảnh
        range_size = 5.0 / numberofpartitions

        # Tìm phân mảnh phù hợp với rating
        partition_index = None
        for i in range(numberofpartitions):
            start = i * range_size
            end = (i + 1) * range_size
            if i == 0:
                if start <= rating <= end:
                    partition_index = i
                    break
            else:
                if start < rating <= end:
                    partition_index = i
                    break

        # Nếu không tìm thấy phân mảnh phù hợp, ném ngoại lệ
        if partition_index is None:
            raise ValueError("Không tìm thấy phân mảnh phù hợp cho rating.")
        
        # Tạo tên bảng phân mảnh chứa giá trị rating
        partition_table = f"{RANGE_TABLE_PREFIX}{partition_index}"
        partition_table_identifier = psycopg2.sql.Identifier(partition_table)

        # Chèn dữ liệu vào bảng phân mảnh tương ứng
        current.execute(
            psycopg2.sql.SQL("INSERT INTO {} ({}) VALUES (%s, %s, %s)").format(
                partition_table_identifier,
                select_columns_sql
            ),
            (userid, itemid, rating)
        )
        openconnection.commit()
        print("Thêm dữ liệu vào bảng phân mảnh thành công!")
    except psycopg2.Error as e:
        print(f"Lỗi PostgreSQL trong rangeinsert: {e}")
        openconnection.rollback()
        raise
    except Exception as ex:
        print(f"Lỗi không xác định trong rangeinsert: {ex}")
        traceback.print_exc()
        openconnection.rollback()
        raise
    finally:
        current.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    print(f"Bắt đầu roundrobinpartition bảng '{ratingstablename}' thành {numberofpartitions} phân mảnh.")
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        print("Số lượng phân mảnh phải là một số nguyên dương.")
        raise ValueError("Số lượng phân mảnh phải là một số nguyên dương.")
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    try:
        # Lấy schema động của bảng gốc
        cur.execute(
            psycopg2.sql.SQL(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position;")
            , (ratingstablename,)
        )
        columns_schema = cur.fetchall()
        if not columns_schema:
            print(f"LỖI: Không tìm thấy bảng '{ratingstablename}' trong database hoặc bảng không có cột nào.")
            raise Exception(f"Bảng '{ratingstablename}' không tồn tại hoặc không có cột.")

        # Xác định tên cột và kiểu dữ liệu của bảng gốc
        column_definitions_sql = psycopg2.sql.SQL(", ").join(
            [psycopg2.sql.SQL("{} {}").format(psycopg2.sql.Identifier(col_name), psycopg2.sql.SQL(col_type))
             for col_name, col_type in columns_schema]
        )
        actual_column_names = [psycopg2.sql.Identifier(col_name) for col_name, _ in columns_schema]
        select_column_names_sql = psycopg2.sql.SQL(", ").join(actual_column_names)
        if not select_column_names_sql.as_string(con).strip():
            print(f"LỖI: Không thể tạo danh sách cột để SELECT từ bảng '{ratingstablename}'.")
            raise Exception(f"Không thể tạo danh sách cột từ bảng '{ratingstablename}'.")

        # Tạo N bảng phân mảnh
        for i in range(numberofpartitions):
            #Tạo tên bảng phân mảnh
            part_table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            part_table_identifier = psycopg2.sql.Identifier(part_table_name)

            #Tạo bảng phân mảnh
            cur.execute(psycopg2.sql.SQL("DROP TABLE IF EXISTS {};").format(part_table_identifier))
            cur.execute(psycopg2.sql.SQL("CREATE TABLE {} ({});").format(part_table_identifier, column_definitions_sql))

            #Thêm dữ liệu vào bảng phân mảnh tương ứng
            sql_insert = psycopg2.sql.SQL("""
                INSERT INTO {} ({})
                SELECT {}
                FROM (
                    SELECT *, ROW_NUMBER() OVER () as rn
                    FROM {}
                ) AS temp_table_with_row_numbers
                WHERE (temp_table_with_row_numbers.rn - 1) %% %s = %s;
            """).format(
                part_table_identifier,
                select_column_names_sql,
                select_column_names_sql,
                psycopg2.sql.Identifier(ratingstablename)
            )
            cur.execute(sql_insert, (numberofpartitions, i))  # Truyền numberofpartitions và i
        con.commit()
        print(f"Hàm roundrobinpartition cho bảng '{ratingstablename}' thành công.")
    except psycopg2.Error as e:
        print(f"LỖI PostgreSQL trong roundrobinpartition: {e}")
        if con: con.rollback()
        raise
    except Exception as ex:
        print(f"LỖI không xác định trong roundrobinpartition: {ex}")
        traceback.print_exc()
        if con: con.rollback()
        raise

def roundrobininsert(ratingstablename, userid, itemid, rating_value, openconnection):
    print(
        f"Bắt đầu roundrobininsert: UserID={userid}, ItemID={itemid}, Rating={rating_value} vào bảng '{ratingstablename}'.")
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    try:
        #Kiểm tra giá trị dữ liệu đầu vào
        if rating_value < 0.0 or rating_value > 5.0:
            raise ValueError(f"Rating không hợp lệ. Phỉa nằm trong khoảng từ 0 đến 5.")

        # Chèn vào bảng Ratings chính (schema: UserID, MovieID, Rating)
        sql_insert_main = psycopg2.sql.SQL("INSERT INTO {} (UserID, MovieID, Rating) VALUES (%s, %s, %s);").format(
            psycopg2.sql.Identifier(ratingstablename)
        )
        cur.execute(sql_insert_main, (userid, itemid, rating_value))

        # Tính tổng hàng sau khi chèn vào bảng chính
        cur.execute(psycopg2.sql.SQL("SELECT COUNT(*) FROM {};").format(psycopg2.sql.Identifier(ratingstablename)))
        total_rows_after_insert = 0
        result = cur.fetchone()
        if result:
            total_rows_after_insert = result[0]
        if total_rows_after_insert == 0:
            print(f"LỖI: Không thể lấy total_rows từ '{ratingstablename}' sau khi chèn.")
            con.rollback()
            raise Exception("Không thể lấy total_rows.")

        # Đếm số phân mảnh đã có
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, con)
        if numberofpartitions == 0:
            print(f"LỖI: Không tìm thấy phân mảnh round robin nào. Hãy tạo phân mảnh trước.")
            con.rollback()
            raise Exception(f"Không có phân mảnh round robin (prefix: {RROBIN_TABLE_PREFIX}).")

        # Xác định phân mảnh đích
        target_partition_index = (total_rows_after_insert - 1) % numberofpartitions
        target_partition_name = f"{RROBIN_TABLE_PREFIX}{target_partition_index}"

        # Chèn vào bảng phân mảnh đích (schema: UserID, MovieID, Rating)
        sql_insert_partition = psycopg2.sql.SQL("INSERT INTO {} (UserID, MovieID, Rating) VALUES (%s, %s, %s);").format(
            psycopg2.sql.Identifier(target_partition_name)
        )
        cur.execute(sql_insert_partition, (userid, itemid, rating_value))
        con.commit()
        print(f"roundrobininsert hoàn thành.")
    except psycopg2.Error as e:
        print(f"LỖI PostgreSQL trong roundrobininsert: {e}")
        if con: con.rollback()
        raise
    except Exception as ex:
        print(f"LỖI không xác định trong roundrobininsert: {ex}")
        traceback.print_exc()
        if con: con.rollback()
        raise

def count_partitions(prefix, openconnection):
    """Đếm số bảng có tiền tố @prefix trong schema public."""
    count = 0
    temp_cur = None
    try:
        temp_cur = openconnection.cursor()
        query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE %s;"
        temp_cur.execute(query, (prefix + '%',))
        result = temp_cur.fetchone()
        if result:
            count = result[0]
    except psycopg2.Error as e:
        print(f"Lỗi khi đếm partitions với prefix '{prefix}': {e}")
    finally:
        if temp_cur:
            temp_cur.close()
    return count