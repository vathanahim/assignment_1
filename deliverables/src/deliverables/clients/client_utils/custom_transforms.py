def generate_input(output):
    return (f"create table {output} (rcid int, company varchar(512));"
            f"insert into {output} values (12843, 'Revelio Labs'), (8030, 'Jane Street');")
