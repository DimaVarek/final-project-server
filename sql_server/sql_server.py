import datetime
import sqlite3
from date_worker.date_worker import get_last_six_months, get_last_four_week, get_last_week
from config_data import STATUS_STAGE, TYPE_STAGE
DB_NAME = "position_db.db"
CREATE_POSITIONS = '''CREATE TABLE POSITIONS
                    (ID INT PRIMARY KEY     NOT NULL,
                    OWNERID         INT     NOT NULL,
                    POSITIONLINK    TEXT    NOT NULL,
                    COMPANYNAME     TEXT    NOT NULL,
                    POSITIONNAME    TEXT    NOT NULL,
                    COMPANYIMAGE    TEXT    NOT NULL,
                    STARTDATE       INT     NOT NULL,
                    CHANGEDATE      INT     NOT NULL);'''

CREATE_INTERVIEWSTAGES = '''CREATE TABLE INTERVIEWSTAGES
                    (ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    POSITIONID      INT     NOT NULL,
                    NUMBERINORDER   INT     NOT NULL,
                    INTERVIEWTYPE   INT     NOT NULL,
                    INTERVIEWSTATUS INT     NOT NULL,
                    COMMENT         TEXT    NOT NULL,
                    DATE            INT);'''

CREATE_DESCRIPTIONS = '''CREATE TABLE DESCRIPTIONS
                    (ID INT PRIMARY KEY     NOT NULL,
                    DESCRIPTION     TEXT    NOT NULL);'''


def _parse_position(position, cur):
    curr_pos = {
        "id": position[0],
        "owner_id": position[1],
        "position_link": position[2],
        "company_name": position[3],
        "position_name": position[4],
        "company_image": position[5],
        "start_date": position[6],
        "change_date": position[7],

        "description": cur.execute(f"SELECT DESCRIPTION FROM DESCRIPTIONS WHERE ID={position[0]}")
        .fetchall()[0][0]
    }
    stages = cur.execute(f"SELECT * FROM INTERVIEWSTAGES WHERE POSITIONID={position[0]}").fetchall()
    stages.sort(key=lambda x: x[2])
    stages = map(lambda x: {'stage_id': x[0], 'type': x[3], "status": x[4], 'comment': x[5], "date": x[6]}, stages)
    curr_pos['interview_stages'] = list(stages)
    return curr_pos


class SqlServer:
    def __init__(self, db_name):
        self.db_name = db_name
        connection = self.connect()
        try:
            connection.execute('SELECT * FROM POSITIONS limit 5')
            print('table POSITIONS exist')
        except sqlite3.OperationalError:
            connection.execute(CREATE_POSITIONS)
            print('table POSITIONS created')

        try:
            connection.execute('SELECT * FROM INTERVIEWSTAGES limit 5')
            print('table INTERVIEWSTAGES exist')
        except sqlite3.OperationalError:
            connection.execute(CREATE_INTERVIEWSTAGES)
            print('table INTERVIEWSTAGES created')

        try:
            connection.execute('SELECT * FROM DESCRIPTIONS limit 5')
            print('table DESCRIPTIONS exist')
        except sqlite3.OperationalError:
            connection.execute(CREATE_DESCRIPTIONS)
            print('table DESCRIPTIONS created')
        connection.close()

    def connect(self):
        return sqlite3.connect(self.db_name)

    def _next_id(self, who_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT max({who_id}) from POSITIONS")
        rows = cur.fetchall()
        if rows[0][0] != None:
            r_id = rows[0][0] + 1
        else:
            r_id = 0
        return r_id

    def _delete_description_by_description_id(self, description_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM DESCRIPTIONS WHERE ID={description_id}")
        conn.commit()
        conn.close()

    def _delete_stages_by_position_id(self, position_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM INTERVIEWSTAGES WHERE POSITIONID={position_id}")
        conn.commit()
        conn.close()

    def _add_description(self, position_id, description_text):
        conn = self.connect()
        cur = conn.cursor()
        description = (position_id, description_text)
        insert_into_description = f"INSERT INTO DESCRIPTIONS (ID, DESCRIPTION) VALUES (?,?);"
        cur.execute(insert_into_description, description)
        conn.commit()
        conn.close()

    def _add_stages(self, position_id, interview_stages):
        conn = self.connect()
        cur = conn.cursor()
        insert_into_interview_stages = f"INSERT INTO INTERVIEWSTAGES (POSITIONID, NUMBERINORDER, INTERVIEWTYPE, " \
                                       f"INTERVIEWSTATUS, COMMENT, DATE) VALUES (?,?,?,?,?,?);"
        for i in range(len(interview_stages)):
            interview_stage = (position_id, i, interview_stages[i]["type"], interview_stages[i]["status"],
                               interview_stages[i]["comment"], int(interview_stages[i]["date"]))
            cur.execute(insert_into_interview_stages, interview_stage)
        conn.commit()
        conn.close()

    def delete_position_by_id(self, position_id):
        conn = self.connect()
        cur = conn.cursor()
        self._delete_description_by_description_id(position_id)
        self._delete_stages_by_position_id(position_id)
        cur.execute(f"DELETE FROM POSITIONS WHERE ID={position_id}")
        conn.commit()
        conn.close()

    def get_positions(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM POSITIONS")
        positions = cur.fetchall()
        result = []
        for position in positions:
            result.append(_parse_position(position, cur))
        conn.close()
        return result

    def get_position_by_id(self, position_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM POSITIONS WHERE ID={position_id}")
        position = cur.fetchall()[0]
        position = _parse_position(position, cur)
        conn.close()
        return position

    def change_position(self, position_id, **kwargs):
        result = -1
        try:
            change_date = datetime.datetime.today().timestamp()
            description_text = kwargs["description"]
            interview_stages = kwargs["interview_stages"]
            # fast fix - must change in normal way later
            try:
                image = kwargs["company_image"]
            except:
                image = ""
            # finish of fast fix
            position = (kwargs["position_link"], kwargs["company_name"], kwargs["position_name"],
                        image, change_date, position_id)

            self._delete_description_by_description_id(position_id)
            self._delete_stages_by_position_id(position_id)

            self._add_description(position_id, description_text)
            self._add_stages(position_id, interview_stages)

            conn = self.connect()
            cur = conn.cursor()
            update_position = '''UPDATE POSITIONS
                                 SET POSITIONLINK = ? ,
                                     COMPANYNAME = ? ,
                                     POSITIONNAME = ? ,
                                     COMPANYIMAGE = ? ,
                                     CHANGEDATE = ? 
                                 WHERE ID = ?'''
            cur.execute(update_position, position)
            conn.commit()
            conn.close()
            result = position_id

        except Exception:
            result = -1

        finally:
            return result

    def add_position(self, **kwargs):
        result = -1
        try:
            start_date = datetime.datetime.now().timestamp()
            change_date = datetime.datetime.now().timestamp()
            position_id = self._next_id("ID")
            description_text = kwargs["description"]
            interview_stages = kwargs["interview_stages"]
            # fast fix - must change in normal way later
            try:
                image = kwargs["company_image"]
            except:
                image = ""
            # finish of fast fix
            position = (position_id, kwargs["owner_id"], kwargs["position_link"], kwargs["company_name"],
                        kwargs["position_name"], image, start_date, change_date)
            self._add_description(position_id, description_text)
            self._add_stages(position_id, interview_stages)

            conn = self.connect()
            cur = conn.cursor()
            insert_into_positions = f"INSERT INTO POSITIONS (ID, OWNERID, POSITIONLINK, COMPANYNAME, POSITIONNAME," \
                                    f"COMPANYIMAGE, STARTDATE, CHANGEDATE) " \
                                    f"VALUES (?,?,?,?,?,?,?,?);"
            cur.execute(insert_into_positions, position)
            conn.commit()
            conn.close()
            result = position_id
        except Exception:
            result = -1
        finally:
            return result

    def get_stages_from_range(self, start_interval, end_interval, owner_id=1):
        sql_request = '''SELECT POSITIONS.ID, INTERVIEWTYPE, INTERVIEWSTATUS, 
                                COMMENT, DATE, COMPANYNAME, POSITIONNAME
                         FROM INTERVIEWSTAGES 
                         JOIN POSITIONS 
                         on POSITIONS.ID = INTERVIEWSTAGES.POSITIONID
                         WHERE DATE >= ? AND DATE <= ? AND OWNERID = ?'''
        con = self.connect()
        cur = con.cursor()
        cur.execute(sql_request, (start_interval, end_interval, owner_id))
        stages = cur.fetchall()
        result_stages = []
        for stage in stages:
            result_stages.append({
                "position_id": stage[0],
                "type": stage[1],
                "status": stage[2],
                "comment": stage[3],
                "date": stage[4],
                "company_name": stage[5],
                "position_name": stage[6]
            })
        con.close()
        return result_stages

    def statistic_applications_last_six_months(self, owner_id=1):
        con = self.connect()
        cur = con.cursor()
        last_six_months_stat = []
        sql_request = '''SELECT COUNT(*) FROM POSITIONS
                         WHERE STARTDATE >= ? AND STARTDATE <= ? AND OWNERID = ?'''
        stats_dates = get_last_six_months()
        for month_id in range(len(stats_dates)):
            cur.execute(sql_request, (stats_dates[month_id][1], stats_dates[month_id][2], owner_id))
            last_six_months_stat.append({
                'id': month_id + 1,
                'month': stats_dates[month_id][0],
                'applications': cur.fetchall()[0][0]
            })
        con.close()
        return last_six_months_stat

    def get_applications_made_last_month(self, owner_id=1):
        con = self.connect()
        cur = con.cursor()
        last_four_week_stat = []
        sql_request = '''SELECT COUNT(*) FROM POSITIONS
                         WHERE STARTDATE >= ? AND STARTDATE <= ? AND OWNERID = ?'''
        stats_dates = get_last_four_week()
        for week_id in range(len(stats_dates)):
            cur.execute(sql_request, (stats_dates[week_id][1], stats_dates[week_id][2], owner_id))
            last_four_week_stat.append({
                # 'id': week_id + 1,
                'week': stats_dates[week_id][0],
                'applications': cur.fetchall()[0][0]
            })
        con.close()
        return last_four_week_stat

    def get_applications_made_last_week(self, owner_id=1):
        con = self.connect()
        cur = con.cursor()
        last_four_week_stat = []
        sql_request = '''SELECT COUNT(*) FROM POSITIONS
                         WHERE STARTDATE >= ? AND STARTDATE <= ? AND OWNERID = ?'''
        stats_dates = get_last_week()
        for day_id in range(len(stats_dates)):
            cur.execute(sql_request, (stats_dates[day_id][1], stats_dates[day_id][2], owner_id))
            last_four_week_stat.append({
                # 'id': week_id + 1,
                'day': stats_dates[day_id][0],
                'applications': cur.fetchall()[0][0]
            })
        con.close()
        return last_four_week_stat

    def total_positive_result_by_each_stage(self, owner_id=1):
        con = self.connect()
        cur = con.cursor()
        stages_state = []
        sql_request = '''SELECT COUNT(*) FROM INTERVIEWSTAGES 
                         JOIN POSITIONs
                         on POSITIONS.ID = INTERVIEWSTAGES.POSITIONID 
                         WHERE OWNERID = ? AND INTERVIEWTYPE = ? AND INTERVIEWSTATUS = ?'''
        for interview_type in TYPE_STAGE:
            cur.execute(sql_request, (owner_id, interview_type, STATUS_STAGE['Accepted']))
            stages_state.append({'stage': interview_type, 'applications': cur.fetchall()[0][0]})
        con.close()
        return stages_state





a = SqlServer(DB_NAME)
# print(a.total_positive_result_by_each_stage())
# interview_stages = [
#     {"type": 'phone',
#      "comment": "It will be exacted!",
#      "status": "exepted",
#      "date": 1693526400},
#     {"type": 'tech',
#      "status": "exepted",
#      "comment": "It will be exacted!",
#      "date": 1693267200},
#     {"type": 'hr',
#      "status": "exepted",
#      "comment": "It will be exacted!",
#      "date": 1691625600}
# ]
# position = {
#     'owner_id': 1,
#     'position_link': 'google.com',
#     'company_name': 'intel',
#     'position_name': 'programmer',
#     "company_image": "google",
#     'description': 'it`s good position',
#     'interview_stages': interview_stages
#     }
# print(a.add_position(**position))
# # print(a.delete_position_by_id(0))
# result = a.get_positions()
# for row in result:
#     print(row)
print(a.get_stages_from_range(1690848000, 1693353600))

# conn = sqlite3.connect(DB_NAME)
# conn.execute("DROP TABLE POSITIONS")
# conn.execute("DROP TABLE DESCRIPTIONS")
# conn.execute("DROP TABLE INTERVIEWSTAGES")
# conn.close()