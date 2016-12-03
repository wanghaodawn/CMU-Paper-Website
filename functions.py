"""
Name: Hao Wang
AndrewID: haow2
"""

import psycopg2 as psy

from datetime import datetime
from pytz import timezone
from constants import *
import traceback
import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf8')


"""
General rules:
    (1) How will the WebApp call these APIs?
    Say we have an API foo(...) defined in this file. The upper layer Application
    will invoke these API through a wrapper in the following way:

    database_wrapper(...):
        conn = None
        try:
            conn = psycopg2.connection()
            res = foo(conn, ...)
            return res
        except psycopg2.DatabaseError, e:
            print "Error %s: " % e.argsp[0]
            conn.rollback()
            return DB_ERROR, None
        finally:
            if conn:
                conn.close()

    So, you don't need to care about the establishment and termination of
    the database connection, we will pass it as a parameter to the api.

    (2) General pattern for return value.
    Return value of every API defined here is a two element tuples (status, res)
.
    Status indicates whether the API call is success or not. Status = 0 means success,
    otherwise the web app will identify the error type by the value of the status.

    Res is the actual return value from the API. If the API has no return value, it should be
    set to None. Otherwise it could be any python data structures or primitives.
"""


def example_select_current_time(conn):
    """
    Example: Get current timestamp from the database

    :param conn: A postgres database connection object
    :return: (status, retval)
        (0, dt)     Success, retval is a python datetime object
        (1, None)           Failure
    """
    try:
        # establish a cursor
        cur = conn.cursor()
        # execute a query
        cur.execute("SELECT localtimestamp")
        # get back result tuple
        res = cur.fetchone()
        # extract the result as a datetime object
        dt = res[0]
        # return the status and result
        return 0, dt
    except psy.DatabaseError, e:
        # catch any database exception and return failure status
        return 1, None


# Admin APIs



# T.1
def reset_db(conn):
    """
    Reset the entire database.
    Delete all tables and then recreate them.

    :param conn: A postgres database connection object
    :return: (status, retval)
        (0, None)   Success
        (1, None)   Failure
    """
    print("[BEGIN] reset_db")
    commands = (
        """
        DROP TABLE IF EXISTS tags, tagnames, likes, papers, users
        """,
        """
        CREATE TABLE IF NOT EXISTS users(
            username VARCHAR(50) NOT NULL,
            password VARCHAR(32) NOT NULL,
            PRIMARY KEY(username)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS papers(
            pid  SERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            title VARCHAR(50),
            begin_time TIMESTAMP NOT NULL,
            description VARCHAR(500),
            data TEXT,
            FOREIGN KEY(username) REFERENCES users ON DELETE CASCADE
        );
        """,
        """
        CREATE INDEX paper_text_idx ON papers USING gin(to_tsvector('english', data))
        """,
        """
        CREATE TABLE IF NOT EXISTS tagnames(
            tagname VARCHAR(50) NOT NULL,
            PRIMARY KEY(tagname)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS likes(
            pid INT NOT NULL,
            username VARCHAR(50) NOT NULL,
            like_time TIMESTAMP NOT NULL,
            PRIMARY KEY(pid, username),
            FOREIGN KEY(pid) REFERENCES papers ON DELETE CASCADE,
            FOREIGN KEY(username) REFERENCES users ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tags(
            pid INT NOT NULL,
            tagname VARCHAR(50) NOT NULL,
            PRIMARY KEY(pid, tagname),
            FOREIGN KEY(pid) REFERENCES papers ON DELETE CASCADE,
            FOREIGN KEY(tagname) REFERENCES tagnames ON DELETE CASCADE
        );
        """,
    )
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    conn.commit()
    return 0, None


# Basic APIs

# Check whether tables have been created
def check_tables(conn):
    print("[BEGIN] check_tables")

    # Set autocommit as false
    conn.autocommit = False

    try:
        cur = conn.cursor()
        cur.execute("""SELECT COUNT(*) 
                       FROM information_schema.tables 
                       WHERE table_name = 'users' OR 
                             table_name = 'papers' OR 
                             table_name = 'tagnames' OR 
                             table_name = 'likes' OR 
                             table_name = 'tags';""")
        res = cur.fetchone()[0]

        print(res)

        if res != 5:
            reset_db(conn)
    except psy.DatabaseError, e:
        traceback.print_exc()


# T.2
def signup(conn, uname, pwd):
    """
    Register a user with a username and password.
    This function first check whether the username is used. If not, it
    registers the user in the users table.

    :param conn:  A postgres database connection object
    :param uname: A string of username
    :param pwd: A string of user's password
    :return: (status, retval)
        (0, None)   Success
        (1, None)   Failure -- Username is used
        (2, None)   Failure -- Other errors
    """
    print("[BEGIN] signup")

    # Check table exists or not
    check_tables(conn)
    
    try:
        cur = conn.cursor()
        cur.execute("""SELECT * FROM users WHERE username = %s;""", (uname, ))
        init_record = cur.fetchall()
        if len(init_record) > 0:
            # username is used
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 2, None

    try:
        cur.execute("""INSERT INTO users (username, password) VALUES (%s, %s);""", (uname, pwd, ))
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 2, None

    # Success
    conn.commit()
    return 0, None


# T.3
def login(conn, uname, pwd):
    """
    Login if user and password match.

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param pwd: A string of user's password
    :return: (status, retval)
        (0, None)   Success
        (1, None)   Failure -- User does not exist
        (2, None)   Failure -- Password incorrect
        (3, None)   Failure -- Other errors
    """
    print("[BEGIN] login")

    # Check table exists or not
    check_tables(conn)
    
    try:
        cur = conn.cursor()
        cur.execute("""SELECT * FROM users WHERE username = %s;""", (uname, ))
        init_record = cur.fetchall()
        if len(init_record) == 0:
            # username does not exist
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 3, None

    try:
        cur.execute("""SELECT * FROM users WHERE username = %s AND password = %s;""", (uname, pwd, ))
        init_record = cur.fetchall()
        if len(init_record) == 0:
            # password incorrect
            return 2, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 3, None

    # Success
    conn.commit()
    return 0, None


# Event related

# If a tag in tags contain any non-alphanumeric characters, then remove it
def check_tags(tags):
    print("[BEGIN] check_tags")
    res = []

    for tag in tags:
        temp = ""
        for c in tag:
            if (c >= '0' and c <= '9') or (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z'):
                temp += c
            else:
                continue;
        res.append(temp)
    return res;


# T.4
def add_new_paper(conn, uname, title, desc, text, tags):
    """
    Create a new paper with  tags.
    Note that this API should touch multiple tables.
    Make sure you define a transaction properly. Also, don't forget to set the begin_time
    of the paper as current time.

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param title: A string of the title of the paper
    :param desc: A string of the description of the paper
    :param text: A string of the text content of the uploaded pdf file
    :param tags: A list of string, each element is a tag associate to the paper
    :return: (status, retval)
        (0, pid)    Success
                    Return the pid of the newly inserted paper in the res field of the return value
        (1, None)   Failure
    """
    print("[BEGIN] add_new_paper")

    pid = 0;

    # First, create the paper
    try:
        cur = conn.cursor()
        curr_time = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        print(curr_time)
        print(uname + "\t" + title + "\t" + curr_time + "\t" + desc)
        cur.execute("""INSERT INTO papers (username, title, begin_time, description, data) 
                     VALUES (%s, %s, %s, %s, %s);""", (uname, title, curr_time, desc, text, ))
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 1, None

    # Second, get the most recent created paper
    try:
        cur.execute("""SELECT pid FROM papers WHERE username = %s ORDER BY begin_time DESC LIMIT 1;""", (uname, ))
        # It returns a tupe, so need to clairify the index as 0
        pid = cur.fetchone()[0]

        print(pid)
    except psy.DatabaseError, e:
        # Other errors
        conn.rollback()
        traceback.print_exc()
        return 1, None

    # Third, create tags
    tags = check_tags(tags)
    
    for tag in tags:
        # Check whether this tagname exists or not
        try:
            cur.execute("""SELECT * FROM tagnames WHERE tagname = %s;""", (tag, ))
            init_record = cur.fetchall()
            # If does not exist, then create the new tagname
            if len(init_record) == 0:
                try:
                    cur.execute("""INSERT INTO tagnames (tagname) VALUES (%s);""", (tag, ))
                except psy.DatabaseError, e:
                    # Other errors
                    conn.rollback()
                    traceback.print_exc()
                    return 1, None
        except psy.DatabaseError, e:
            # Other errors
            conn.rollback()
            traceback.print_exc()
            return 1, None

        # Insert into tags
        try:
            print(tag)
            cur.execute("""INSERT INTO tags (pid, tagname) VALUES (%s, %s);""", (pid, tag, ))
        except psy.DatabaseError, e:
            # Other errors
            conn.rollback()
            traceback.print_exc()
            return 1, None

    # Success
    conn.commit()
    return 0, pid


# T.5
# It cannot delete a paper who doesn't have a local file, as in views.py
# it will throw an exception when trying os.remove(filename)
def delete_paper(conn, pid):
    """
    Delete a paper by the given pid.

    :param conn: A postgres database connection object
    :param pid: An int of pid
    :return: (status, retval)
        (0, None)   Success
        (1, None)   Failure
    """
    print("[BEGIN] delete_paper")

    init_record = []

    # First, check whether exist this paper or not
    try:
        cur = conn.cursor()
        cur.execute("""SELECT pid FROM papers WHERE pid = %s;""", (pid, ))
        init_record = cur.fetchall()

        # If doesn't exist, fail
        if len(init_record) == 0:
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    # Second, if doesn't exist, then delete
    try:
        cur.execute("""DELETE FROM papers WHERE pid = %s;""", (pid, ))
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 1, None

    print("Success")

    # Success
    return 0, None


# T.10
def get_paper_tags(conn, pid):
    """
    Get all tags of a paper

    :param conn: A postgres database connection object
    :param pid: An int of pid
    :return: (status, retval)
        (0, [tag1, tag2, ...])      Success
                                    Return a list of string. Each string is a tag of the paper.
                                    Note that the list should be sorted in a lexical ascending order.
                                    Example:
                                            (0, ["database", "multi-versioned"])

        (1, None)                   Failure
    """
    print("[BEGIN] get_paper_tags")
    res = []

    # Get all the tags
    try:
        cur = conn.cursor()
        cur.execute("""SELECT * 
                       FROM tags 
                       WHERE pid = %s 
                       ORDER BY tagname ASC;""", (pid, ))
        items = cur.fetchall()

        for item in items:
            res.append(item[1])
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)
    
    # Success
    return 0, res

# Vote related


# T.11 - Like
def like_paper(conn, uname, pid):
    """
    Record a like for a paper. Timestamped the like with the current timestamp

    You need to ensure that (1) a user should not like his/her own paper, (2) a user can not like a paper twice.

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param pid: An int of pid
    :return: (status, retval)
        (0, None)   Success
        (1, None)   Failure
    """
    print("[BEGIN] like_paper")

    # First, check whether it is his paper
    try:
        cur = conn.cursor()
        cur.execute("""SELECT pid FROM papers WHERE pid = %s AND username != %s;""", (pid, uname, ))
        init_record = cur.fetchall()

        print(init_record)

        # If it is his paper, then fail
        if len(init_record) == 0:
            print("1, None")
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        print("11, None")
        return 1, None

    # Second, whether the user has liked the paper
    try:
        cur.execute("""SELECT * FROM likes WHERE pid = %s AND username = %s;""", (pid, uname, ))
        init_record = cur.fetchall()

        print(init_record)

        # If the user has liked the paper, then fail
        if len(init_record) > 0:
            print("111, None")
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        print("1111, None")
        return 1, None

    # Third, like it
    try:
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        cur.execute("""INSERT INTO likes (username, pid, like_time) VALUES (%s, %s, %s);""", (uname, pid, curr_time, ))
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        print("11111, None")
        return 1, None

    # Success
    conn.commit()
    print("0, None")
    return 0, None


# T.11 - Unlike
def unlike_paper(conn, uname, pid):
    """
    Record an unlike for a paper

    You need to ensure that the user calling unlike has liked the paper before

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param pid: An int of pid
    :return: (status, retval)
        (0, None)   Success
        (1, None)   Failure
    """
    print("[BEGIN] unlike_paper")

    # First, check whether it is his paper
    try:
        cur = conn.cursor()
        cur.execute("""SELECT pid FROM papers WHERE pid = %s AND username != %s;""", (pid, uname, ))
        init_record = cur.fetchall()
        # If it is his paper, then fail
        if len(init_record) == 0:
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    # Second, whether the user has liked the paper
    try:
        cur.execute("""SELECT * FROM likes WHERE pid = %s AND username = %s;""", (pid, uname, ))
        init_record = cur.fetchall()
        # If the user hasn't liked the paper, then fail
        if len(init_record) == 0:
            return 1, None
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    # Third, unlike it
    try:
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        cur.execute("""DELETE FROM likes WHERE pid = %s AND username = %s;""", (pid, uname, ))
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        conn.rollback()
        return 1, None

    # Success
    conn.commit()
    return 0, None


# T.12
def get_likes(conn, pid):
    """
    Get the number of likes of a paper

    :param conn: A postgres database connection object
    :param pid: An int of pid
    :return: (status, retval)
        (0, like_count)     Success, retval should be an integer of like count
        (1, None)           Failure
    """
    print("[BEGIN] get_likes")
    count = 0

    try:
        cur = conn.cursor()
        cur.execute("""SELECT COUNT(*) FROM likes WHERE pid = %s;""", (pid, ))
        count = cur.fetchone()[0]
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(count)

    # Success
    return 0, count

# Search related


# T.6
def get_timeline(conn, uname, count = 10):
    """
    Get timeline of a user.

    You should return $count most recent posts of a user. The result should be ordered first by time (newest first)
    and then break ties by pid (ascending).

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param count: An int indicating the maximum number of papers you can return
    :return: (status, retval)
        (0, [(pid, username, title, begin_time, description), (...), ...])
          Success, retval is a list of quintuple. Each element of the quintuple is of the following type:
            pid --  Integer
            username, title, description -- String
            begin_time  -- A datetime.datetime object
            For example, the return value could be:
            (
                0,
                [
                    (1, "Alice", "title", begin_time, "description")),
                    (2, "Alice", "title2", begin_time2, "description2"))
                ]
            )


        (1, None)
            Failure
    """
    print("[BEGIN] get_timeline")
    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT p.pid, p.username, p.title, p.begin_time, p.description 
                       FROM papers p
                       WHERE p.username = %s
                       GROUP BY p.pid, p.username, p.title, p.begin_time, p.description
                       ORDER BY p.begin_time DESC, p.pid ASC
                       LIMIT %s;""", (uname, count, ))
        res = cur.fetchall()
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.7
def get_timeline_all(conn, count = 10):
    """
    Get at most $count recent papers

    The results should be ordered by begin_time (newest first). Break ties by pid.

    :param conn: A postgres database connection object
    :param count: An int indicating the maximum number of papers you can return
    :return: (status, retval)
        (0, [pid, username, title, begin_time, description), (...), ...])
            Success, retval is a list of quintuple. Please refer to the format defined in get_timeline()'s return value

        (1, None)
            Failure
    """
    print("[BEGIN] get_timeline_all")
    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT p.pid, p.username, p.title, p.begin_time, p.description 
                       FROM papers p
                       GROUP BY p.pid, p.username, p.title, p.begin_time, p.description
                       ORDER BY p.begin_time DESC, p.pid ASC
                       LIMIT %s;""", (count, ))
        res = cur.fetchall()
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.14
def get_most_popular_papers(conn, begin_time, count = 10):
    """
    Get at most $count papers posted after $begin_time according that have the most likes.

    You should order papers first by number of likes (descending) and break ties by pid (ascending).
    Also, paper with 0 like should not be listed here.

    :param conn: A postgres database connection object
    :param begin_time: A datetime.datetime object
    :param count:   An integer
    :return: (status, retval)
        (0, [pid, username, title, begin_time, description), (...), ...])
            Success, retval is a list of quintuple. Please refer to the format defined in get_timeline()'s return value

        (1, None)
            Failure
    """
    print("[BEGIN] get_most_popular_papers")
    res = []

    try:
        cur = conn.cursor()
        cur.execute("""WITH paper_leftjoin_like AS (
                       SELECT p.pid, p.username, p.title, p.begin_time, p.description, l.username AS l_username
                       FROM papers p LEFT JOIN likes l ON p.pid = l.pid
                       )
                       SELECT p.pid, p.username, p.title, p.begin_time, p.description,
                          SUM(CASE WHEN l_username IS NOT NULL THEN 1 ELSE 0 END) AS count_likes
                       FROM paper_leftjoin_like p
                       WHERE p.begin_time > %s
                       GROUP BY p.pid, p.username, p.title, p.begin_time, p.description
                       ORDER BY count_likes DESC, p.pid ASC
                       LIMIT %s;""", (begin_time, count, ))
        items = cur.fetchall()

        # Remove number of likes
        for item in items:
            # Remove papers with 0 likes
            if int(item[5]) == 0:
                break
            temp = (item[0], item[1], item[2], item[3], item[4])
            res.append(temp)
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.15 - untested
def get_recommend_papers(conn, uname, count = 10):
    """
    Recommended at most $count papers for a user.

    Check T.15 in the project writeup for detailed description of this API.

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param count:   An integer
    :return:    (status, retval)
        (0, [pid, username, title, begin_time, description), (...), ...])
            Success, retval is a list of quintuple. Please refer to the format defined in get_timeline()'s return value

        (1, None)
            Failure
    """
    print("[BEGIN] get_recommend_papers")
    res = []

    try:
        cur = conn.cursor()
        cur.execute("""WITH cohort_users AS (
                       SELECT u2.username
                       FROM users u1, users u2, likes l1, likes l2
                       WHERE u1.username = %s AND
                          u1.username != u2.username AND
                          u1.username = l1.username AND
                          l1.pid = l2.pid AND
                          u2.username = l2.username
                       GROUP BY u2.username
                       ORDER BY COUNT(*) DESC, u2.username ASC
                       LIMIT 20
                       )
                       SELECT p.pid, p.title, p.username, p.begin_time, p.description, 
                          COUNT(l.username) AS count_cohort_users
                       FROM cohort_users u, papers p, likes l
                       WHERE p.pid = l.pid AND
                       l.username = u.username AND
                       l.pid NOT IN (
                          SELECT l2.pid FROM likes l2 WHERE l2.username = %s
                       )
                       GROUP BY p.pid, p.title, p.username, p.begin_time, p.description
                       ORDER BY count_cohort_users DESC, p.pid ASC
                       LIMIT %s;""", (uname, uname, count, ))
        items = cur.fetchall()

        # Remove count
        for item in items:
            temp = (item[0], item[1], item[2], item[3], item[4])
            res.append(temp)
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    # Success
    return 0, res


# T.9
def get_papers_by_tag(conn, tag, count = 10):
    """
    Get at most $count papers that have the given tag

    The result should first be ordered by begin time (newest first). Break ties by pid (ascending).

    :param conn: A postgres database connection object
    :param tag: A string of tag
    :param count: An integer
    :return:    (status, retval)
        (0, [pid, username, title, begin_time, description), (...), ...])
            Success, retval is a list of quintuple. Please refer to the format defined in get_timeline()'s return value

        (1, None)
            Failure
    """
    print("[BEGIN] get_papers_by_tag")

    if len(tag) <= 0:
        return 1, None

    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT p.pid, p.username, p.title, p.begin_time, p.description
                       FROM papers p, tags t
                       WHERE p.pid = t.pid AND t.tagname = %s
                       GROUP BY p.pid, p.username, p.title, p.begin_time, p.description
                       ORDER BY p.begin_time DESC, p.pid ASC
                       LIMIT %s;""", (tag, count, ))
        res = cur.fetchall()
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.8
def get_papers_by_keyword(conn, keyword, count = 10):
    """
    Get at most $count papers that match a keyword in its title, description *or* text field

    The result should first be ordered by begin time (newest first). Break ties by pid (ascending).

    :param conn: A postgres database connection object
    :param keyword: A string of keyword, e.g. "database"
    :param count: An integer
    :return:    (status, retval)
        (0, [(pid, username, title, begin_time, description), (...), ...])
            Success, retval is a list of quintuple. Please refer to the format defined in get_timeline()'s return value

        (1, None)
            Failure
    """
    print("[BEGIN] get_papers_by_keyword")

    # If keyword is null, then return all papers
    if not keyword or len(keyword) <= 0:
        return get_timeline_all(conn, count)

    res = []

    keyword = '%' + keyword + '%'

    try:
        cur = conn.cursor()
        cur.execute("""SELECT p.pid, p.username, p.title, p.begin_time, p.description
                       FROM papers p
                       WHERE p.title @@ to_tsquery(%s) OR
                          p.data @@ to_tsquery(%s) OR 
                          p.description @@ to_tsquery(%s)
                       GROUP BY p.pid, p.username, p.title, p.begin_time, p.description
                       ORDER BY p.begin_time DESC, p.pid ASC
                       LIMIT %s;""", (keyword, keyword, keyword, count, ))
        res = cur.fetchall()
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    # Success
    return 0, res


# T.13
def get_papers_by_liked(conn, uname, count = 10):
    """
    Get at most $count papers that liked by the given user.

    The result should first be ordered by the time the like is made (newest first). Break ties by pid (ascending).

    :param conn: A postgres database connection object
    :param uname: A string of username
    :param count: An integer
    :return:    (status, retval)
        (0, [(pid, username, title, begin_time, description), (...), ...])
            Success, retval is a list of quintuple. Please refer to the format defined in get_timeline()'s return value

        (1, None)
            Failure
    """
    print("[BEGIN] get_papers_by_liked")

    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT p.pid, p.username, p.title, p.begin_time, p.description
                       FROM papers p, likes l
                       WHERE l.username = %s AND p.pid = l.pid
                       GROUP BY p.pid, p.username, p.title, p.begin_time, p.description
                       ORDER BY p.begin_time DESC, p.pid ASC
                       LIMIT %s;""", (uname, count, ))
        res = cur.fetchall()
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# Statistics related


# T.17(a)
def get_most_active_users(conn, count = 1):
    """
    Get at most $count users that post most papers.

    The result should first be ordered by number of papers posted by the user. Break ties by username (lexically
    ascending). User that never posted papers should not be listed.

    :param conn: A postgres database connection object
    :param count: An integer
    :return: (status, retval)
        (0, [uname1, uname2, ...])
            Success, retval is a list of username. Each element in the list is a string. Return empty list if no
            username found.
        (1, None)
            Failure
    """
    print("[BEGIN] get_most_active_users")

    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT u.username FROM users u, papers p WHERE p.username = u.username
                       GROUP BY u.username ORDER BY COUNT(p.pid) DESC, u.username ASC LIMIT %s;""", (count, ))
        fetch = cur.fetchall()
        if fetch and len(fetch) > 0:
            for item in fetch[0]:
                if item:
                    res.append(item)
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.17(b)
def get_most_popular_tags(conn, count = 1):
    """
    Get at most $count many tags that gets most used among all papers

    The result should first be ordered by number of papers that has the tags. Break ties by tag name (lexically
    ascending).

    :param conn: A postgres database connection object
    :param count: An integer
    :return:
        (0, [(tagname1, count1), (tagname2, count2), ...])
            Success, retval is a list of tagname. Each element is a pair where the first component is the tagname
            and the second one is its count
        (1, None)
            Failure
    """
    print("[BEGIN] get_most_popular_tags")
    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT t.tagname, COUNT(t.pid) FROM tags t GROUP BY t.tagname
                       ORDER BY COUNT(t.pid) DESC, t.tagname ASC LIMIT %s;""", (count, ))
        items = cur.fetchall()

        # Convert long to integer
        for item in items:
            temp = (item[0], int(item[1]))
            res.append(temp)
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.17(c)
def get_most_popular_tag_pairs(conn, count = 1):
    """
    Get at most $count many tag pairs that have been used together.

    You should avoid duplicate pairs like (foo, bar) and (bar, foo). They should only be counted once with lexical
    order. Results should first be ordered by number occurrences in papers. Break ties by tag name (lexically
    ascending).

    :param conn: A postgres database connection object
    :param count: An integer
    :return:
        (0, [(tag11, tag12, count), (tag21, tag22, count), (...), ...])
            Success, retval is a list of three-tuples. The elements of the three-tuple are two strings and a count.

        (1, None)
            Failure
    """
    print("[BEGIN] get_most_popular_tag_pairs")
    res = []

    try:
        cur = conn.cursor()
        cur.execute("""SELECT t1.tagname, t2.tagname, COUNT(*) FROM tags t1, tags t2
                       WHERE t1.tagname < t2.tagname AND t1.pid = t2.pid GROUP BY t1.tagname, t2.tagname
                       ORDER BY COUNT(*) DESC, t1.tagname ASC, t2.tagname ASC LIMIT %s;""", (count, ))
        res = list(cur.fetchall())
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print(res)

    # Success
    return 0, res


# T.16(a)
def get_number_papers_user(conn, uname):
    """
    Get the number of papers posted by a given user.

    :param conn: A postgres database connection object
    :param uname: A string of username
    :return:
        (0, count)
            Success, retval is an integer indicating the number papers posted by the user
        (1, None)
            Failure
    """
    print("[BEGIN] get_number_papers_user")
    count = 0

    try:
        cur = conn.cursor()
        cur.execute("""SELECT COUNT(p.pid) FROM papers p WHERE p.username = %s;""", (uname, ))
        count = cur.fetchone()[0]
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print("16(a)\t" + str(count))

    # Success
    return 0, count


# T.16(b)
def get_number_liked_user(conn, uname):
    """
    Get the number of likes liked by the user

    :param conn: A postgres database connection object
    :param uname:   A string of username
    :return:
        (0, count)
            Success, retval is an integer indicating the number of likes liked by the user
        (1, None)
            Failure
    """
    print("[BEGIN] get_number_liked_user")
    count = 0

    try:
        cur = conn.cursor()
        cur.execute("""SELECT COUNT(l.pid) FROM likes l WHERE l.username = %s;""", (uname, ))
        count = cur.fetchone()[0]
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print("16(b)\t" + str(count))

    # Success
    return 0, count


# T.16(c)
def get_number_tags_user(conn, uname):
    """
    Get the number of distinct tagnames used by the user.

    Note that you need to eliminate the duplication.

    :param conn: A postgres database connection object
    :param uname:  A string of username
    :return:
        (0, count)
            Success, retval is an integer indicating the number of tagnames used by the user
        (1, None)
            Failure
    """
    print("[BEGIN] get_number_tags_user")
    count = 0

    try:
        cur = conn.cursor()
        cur.execute("""SELECT COUNT(DISTINCT(t.tagname)) FROM papers p, tags t 
                       WHERE p.username = %s AND t.pid = p.pid;""", (uname, ))
        count = cur.fetchone()[0]
    except psy.DatabaseError, e:
        # Other errors
        traceback.print_exc()
        return 1, None

    print("16(c)\t" + str(count))

    # Success
    return 0, count
