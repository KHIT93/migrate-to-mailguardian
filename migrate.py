import mysql.connector
import psycopg2
import argparse
import json
import uuid
import datetime
import time

parser = argparse.ArgumentParser()

parser.add_argument('--mailwatch-config', help='The path to conf.php for your MailWatch installation')
parser.add_argument('--mailguardian-config', help='The path to mailguardian-env.json for your MailGuardian installation')

args = parser.parse_args()

mysql_config = {}
pgsql_config = {}
src = {}
mysql_conn = None
pgsql_conn = None
start_time = datetime.datetime.now()
stop_time = None
errors = []

def migrate_mail_message(message):
    pass
        

if __name__ == "__main__":
    with open(args.mailwatch_config, 'r') as f:
        for line in f:
            if "define('DB_USER'," in line:
                mysql_config['username'] = line.replace("define('DB_USER', '", "").replace("');", "").rstrip()
            if "define('DB_PASS'," in line:
                mysql_config['password'] = line.replace("define('DB_PASS', '", "").replace("');", "").rstrip()
            if "define('DB_HOST'," in line:
                mysql_config['host'] = line.replace("define('DB_HOST', '", "").replace("');", "").rstrip()
            if "define('DB_NAME'," in line:
                mysql_config['name'] = line.replace("define('DB_NAME', '", "").replace("');", "").rstrip()

    with open(args.mailguardian_config, 'r') as f:
        data = json.load(f)
        pgsql_config['username'] = data['database']['user']
        pgsql_config['password'] = data['database']['password']
        pgsql_config['host'] = data['database']['host']
        pgsql_config['name'] = data['database']['name']
        pgsql_config['port'] = data['database']['port']
        if 'options' in data['database']:
            if 'sslmode' in data['database']['options']:
                pgsql_config['sslmode'] = data['database']['options']['sslmode']


    try:
        mysql_conn = mysql.connector.connect(host=mysql_config['host'], user=mysql_config['username'], passwd=mysql_config['password'], db=mysql_config['name'])
        mysql_conn2 = mysql.connector.connect(host=mysql_config['host'], user=mysql_config['username'], passwd=mysql_config['password'], db=mysql_config['name'])
    except mysql.connector.Error as e:
        print(e)
        exit()

    print('Successfully connected to source MySQL database for MailWatch' + chr(13))
    
    try:
        pgsql_conn = psycopg2.connect(dbname=pgsql_config['name'], user=pgsql_config['username'], password=pgsql_config['password'], host=pgsql_config['host'], port=pgsql_config['port'], sslmode=pgsql_config['sslmode'])
        pgsql_conn.autocommit = True
    except psycopg2.OperationalError as e:
        print(e)
        exit()

    print('Successfully connected to the destination PostgreSQL database for MailGuardian' + chr(13))
    
    print('Successfully connected to source and destination datastorage' + chr(13))
    #pgsql_cursor = pgsql_conn.cursor()
    mysql_cursor = mysql_conn.cursor()
    print('Getting list of tables from source database' + chr(13))
    mysql_cursor.execute("SHOW TABLES")
    tables = []
    for row in mysql_cursor.fetchall():
        tables.append(row[0])

    # mysql_cursor = mysql_conn.cursor(dictionary=True)
    # print('Counting maillog entries to process' + chr(13))
    # mysql_cursor.execute("SELECT count(id) as id__count FROM maillog")
    # total = mysql_cursor.fetchone()['id__count']
    # count = 0
    # messages_sql = []
    # if total > 100000:
    #     messages_sql.append("SELECT * FROM maillog LIMIT 100000")
    #     to_process = 100000
    #     while to_process < total:
    #         to_add = 100000
    #         if to_process + 100000 > total:
    #             to_add = total - to_process
    #         messages_sql.append("SELECT * FROM maillog LIMIT {0} OFFSET {1}".format(to_add, to_process))
    #         to_process += to_add
    # else:
    #     messages_sql = ["SELECT * FROM maillog"]
    # print('Collecting maillog entries to process' + chr(13))
    # #mysql_cursor.execute("SELECT * FROM maillog")
    # for query in messages_sql:
    #     try:
    #         mysql_cursor = mysql_conn.cursor(dictionary=True)
    #     except:
    #         print('[{0}%] :: Processing message {1} :: MySQL Connection Error. Waiting 300 seconds before retrying'.format(round((count/total) * 100, 2), message['id']))
    #         time.sleep(300)
    #         print('[{0}%] :: Processing message {1} :: MySQL Connection Error. Retrying connection'.format(round((count/total) * 100, 2), message['id']))
    #         mysql_cursor = mysql_conn.cursor(dictionary=True)
    #     mysql_cursor.execute(query)
    #     results = mysql_cursor.fetchall()
    #     mysql_cursor.close()
    #     for message in results:
    #         try:
    #             pgsql_cursor = pgsql_conn.cursor()
    #             print('[{0}%] :: Processing message {1}'.format(round((count/total) * 100, 2), message['id']))
    #             vals = {
    #                 'id': str(uuid.uuid4()),
    #                 'from_address': message['from_address'],
    #                 'from_domain': message['from_domain'],
    #                 'to_address': message['to_address'],
    #                 'to_domain': message['to_domain'],
    #                 'subject': message['subject'] if message['subject'] else "",
    #                 'client_ip': message['clientip'],
    #                 'mailscanner_hostname': message['hostname'],
    #                 'spam_score': message['sascore'] if message['sascore'] else 0.00,
    #                 'mcp_score': message['mcpsascore'] if message['mcpsascore'] else 0.00,
    #                 'timestamp': message['timestamp'],
    #                 'date': message['date'],
    #                 'size': message['size'],
    #                 'token': message['token'],
    #                 'mailq_id': message['id'],
    #                 'whitelisted': True if message['spamwhitelisted'] == 1 else False,
    #                 'blacklisted': True if message['spamblacklisted'] == 1 else False,
    #                 'is_spam': True if message['issaspam'] == 1 or message['isspam'] == 1 or message['ishighspam'] == 1 else False,
    #                 'is_mcp': True if message['issamcp'] == 1 or message['ismcp'] == 1 or message['ishighmcp'] == 1 else False,
    #                 'is_rbl_listed': True if message['isrblspam'] == 1 else False,
    #                 'quarantined': True if message['quarantined'] == 1 else False,
    #                 'infected': True if message['virusinfected'] == 1 or message['nameinfected'] == 1 or message['otherinfected'] == 1 else False,
    #                 'released': True if message['released'] == 1 else False
    #             }
    #             pgsql_cursor.execute("INSERT INTO mail_message (id, from_address, from_domain, to_address, to_domain, subject, client_ip, mailscanner_hostname, spam_score, timestamp, token, whitelisted, blacklisted, is_spam, is_rbl_listed, quarantined, infected, size, mailq_id, is_mcp, mcp_score, date, released) VALUES(%(id)s, %(from_address)s, %(from_domain)s, %(to_address)s, %(to_domain)s, %(subject)s, %(client_ip)s, %(mailscanner_hostname)s, %(spam_score)s, %(timestamp)s, %(token)s, %(whitelisted)s, %(blacklisted)s, %(is_spam)s, %(is_rbl_listed)s, %(quarantined)s, %(infected)s, %(size)s, %(mailq_id)s, %(is_mcp)s, %(mcp_score)s, %(date)s, %(released)s) RETURNING id", (vals))
    #             message_id = pgsql_cursor.fetchone()[0]
    #             vals = {
    #                 'id': str(uuid.uuid4()),
    #                 'contents': message['headers'],
    #                 'message_id': message_id
    #             }
    #             pgsql_cursor.execute("INSERT INTO mail_headers (id, contents, message_id) VALUES (%(id)s, %(contents)s, %(message_id)s)", (vals))
    #             vals = {
    #                 'id': str(uuid.uuid4()),
    #                 'contents': message['report'],
    #                 'message_id': message_id
    #             }
    #             pgsql_cursor.execute("INSERT INTO mail_mailscannerreport (id, contents, message_id) VALUES (%(id)s, %(contents)s, %(message_id)s)", (vals))
    #             vals = {
    #                 'id': str(uuid.uuid4()),
    #                 'contents': message['mcpreport'],
    #                 'message_id': message_id
    #             }
    #             pgsql_cursor.execute("INSERT INTO mail_mcpreport (id, contents, message_id) VALUES (%(id)s, %(contents)s, %(message_id)s)", (vals))
    #             vals = {
    #                 'id': str(uuid.uuid4()),
    #                 'contents': message['rblspamreport'],
    #                 'message_id': message_id
    #             }
    #             pgsql_cursor.execute("INSERT INTO mail_rblreport (id, contents, message_id) VALUES (%(id)s, %(contents)s, %(message_id)s)", (vals))
    #             vals = {
    #                 'id': str(uuid.uuid4()),
    #                 'contents': message['report'],
    #                 'message_id': message_id
    #             }
    #             pgsql_cursor.execute("INSERT INTO mail_spamreport (id, contents, message_id) VALUES (%(id)s, %(contents)s, %(message_id)s)", (vals))
    #             transport_log_cursor = mysql_conn2.cursor(dictionary=True)
    #             transport_log_cursor.execute("SELECT * FROM mtalog WHERE msg_id='{0}'".format(message_id))
    #             for entry in transport_log_cursor:
    #                 vals = {
    #                     'id': str(uuid.uuid4()),
    #                     'timestamp': entry['timestamp'],
    #                     'message_id': message_id,
    #                     'transport_host': entry['host'],
    #                     'transport_type': entry['type'],
    #                     'relay_host': entry['relay'],
    #                     'dsn': entry['dsn'],
    #                     'dsn_message': entry['status'],
    #                     'delay': entry['delay']
    #                 }
    #                 pgsql_cursor.execute("INSERT INTO mail_transportlog (id, timestamp, message_id, transport_host, transport_type, relay_host, dsn, dsn_message, delay) VALUES(%(id)s, %(timestamp)s, %(message_id)s, %(transport_host)s, %(transport_type)s, %(relay_host)s, %(dsn)s, %(dsn_message)s, %(delay)s)", (vals))
    #             pgsql_conn.commit()
    #             count += 1
    #             transport_log_cursor.close()
    #         except Exception as e:
    #             errors.append(e)
    #         finally:
    #             pgsql_cursor.close()
    # pgsql_cursor.close()
    # mysql_cursor.close()
    pgsql_cursor = pgsql_conn.cursor()
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    mysql_cursor.execute("SELECT count(id) as id__count FROM blacklist")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    mysql_cursor.execute("SELECT * FROM blacklist")
    for entry in mysql_cursor:
        print('[{0}%] :: Processing blacklist {1}'.format(round((count/total) * 100, 2), entry['id']))
        vals = {
            'id': str(uuid.uuid4()),
            'from_address': '*' if entry['from_address'] == 'default' else entry['from_address'],
            'to_address': '*' if entry['to_address'] == 'default' else entry['to_address'],
            'to_domain': (entry['to_domain'] if entry['to_domain'] != 'default' else '*') if entry['to_domain'] else '',
            'listing_type': 'blacklisted'
        }
        pgsql_cursor.execute("INSERT INTO list_entries (id, from_address, to_address, to_domain, listing_type) VALUES(%(id)s, %(from_address)s, %(to_address)s, %(to_domain)s, %(listing_type)s)", (vals))
        pgsql_conn.commit()
        count += 1
    pgsql_cursor.close()
    mysql_cursor.close()
    pgsql_cursor = pgsql_conn.cursor()
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    mysql_cursor.execute("SELECT count(id) as id__count FROM blacklist")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    mysql_cursor.execute("SELECT * FROM whitelist")
    for entry in mysql_cursor:
        print('[{0}%] :: Processing whitelist {1}'.format(round((count/total) * 100, 2), entry['id']))
        vals = {
            'id': str(uuid.uuid4()),
            'from_address': '*' if entry['from_address'] == 'default' else entry['from_address'],
            'to_address': '*' if entry['to_address'] == 'default' else entry['to_address'],
            'to_domain': (entry['to_domain'] if entry['to_domain'] != 'default' else '*') if entry['to_domain'] else '',
            'listing_type': 'whitelisted'
        }
        pgsql_cursor.execute("INSERT INTO list_entries (id, from_address, to_address, to_domain, listing_type) VALUES(%(id)s, %(from_address)s, %(to_address)s, %(to_domain)s, %(listing_type)s)", (vals))
        pgsql_conn.commit()
        count += 1
    pgsql_cursor.close()
    mysql_cursor.close()
    if 'smtpaccess' in tables:
        pgsql_cursor = pgsql_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute("SELECT count(id) as id__count FROM smtpaccess")
        total = mysql_cursor.fetchone()['id__count']
        count = 0
        mysql_cursor.execute("SELECT * FROM smtpaccess")
        for entry in mysql_cursor:
            print('[{0}%] :: Processing SMTP relay {1}'.format(round((count/total) * 100, 2), entry['id']))
            vals = {
                'id': str(uuid.uuid4()),
                'ip_address': entry['smtpvalue'],
                'comment': entry['comment'],
                'active': True,
                'hostname': entry['smtpvalue']
            }
            pgsql_cursor.execute("INSERT INTO mail_smtprelay (id, ip_address, comment, active, hostname) VALUES(%(id)s, %(ip_address)s, %(comment)s, %(active)s, %(hostname)s)", (vals))
            pgsql_conn.commit()
            count += 1
        pgsql_cursor.close()
        mysql_cursor.close()
    if 'domaintable' in tables:
        pgsql_cursor = pgsql_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute("SELECT count(domainname) as id__count FROM domaintable")
        total = mysql_cursor.fetchone()['id__count']
        count = 0
        mysql_cursor.execute("SELECT * FROM domaintable")
        for entry in mysql_cursor:
            print('[{0}%] :: Processing domain {1}'.format(round((count/total) * 100, 2), entry['domainname']))
            vals = {
                'id': str(uuid.uuid4()),
                'name': entry['domainname'],
                'destination': entry['relaymap'],
                'relay_type': entry['relaytype'],
                'created_timestamp': entry['createdts'],
                'updated_timestamp': entry['createdts'],
                'active': True,
                'catchall': True,
                'allowed_accounts': entry['accountno'],
                'receive_type': 'failover'
            }
            pgsql_cursor.execute("INSERT INTO domains_domain (id, name, destination, relay_type, created_timestamp, updated_timestamp, active, catchall, allowed_accounts, receive_type) VALUES(%(id)s, %(name)s, %(destination)s, %(relay_type)s, %(created_timestamp)s, %(updated_timestamp)s, %(active)s, %(catchall)s, %(allowed_accounts)s, %(receive_type)s)", (vals))
            pgsql_conn.commit()
            count += 1
    pgsql_cursor = pgsql_conn.cursor()
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    mysql_cursor.execute("SELECT count(id) as id__count FROM users")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    mysql_cursor.execute("SELECT * FROM users")
    for user in mysql_cursor:
        print('[{0}%] :: Processing user {1}'.format(round((count/total) * 100, 2), user['id']))
        vals = {
            'id': str(uuid.uuid4()),
            'email': user['username'] if '@' in user['username'] else 'admin@' + user['username'],
            'first_name': user['fullname'],
            'last_name': '',
            'password': '',
            'is_domain_admin': True if user['type'] == 'D' else False,
            'is_staff': True if user['type'] == 'A' else False,
            'is_superuser': True if user['type'] == 'A' else False,
            'is_active': True,
            'daily_quarantine_report': True if user['quarantine_report'] == 1 else False,
            'weekly_quarantine_report': False,
            'monthly_quarantine_report': False,
            'custom_spam_score': 5 if user['spamscore'] == 0 else user['spamscore'],
            'custom_spam_highscore': 15 if user['highspamscore'] == 0 else user['highspamscore'],
            'skip_scan': True if user['noscan'] == 1 else False,
            'last_login': None if user['last_login'] == -1 else datetime.datetime.utcfromtimestamp(user['last_login']),
            'date_joined': datetime.datetime.now(),
        }
        pgsql_cursor.execute("INSERT INTO core_user (id, email, first_name, last_name, password, is_domain_admin, is_staff, is_superuser, is_active, daily_quarantine_report, weekly_quarantine_report, monthly_quarantine_report, custom_spam_score, custom_spam_highscore, skip_scan, last_login, date_joined) VALUES(%(id)s, %(email)s, %(first_name)s, %(last_name)s, %(password)s, %(is_domain_admin)s, %(is_staff)s, %(is_superuser)s, %(is_active)s, %(daily_quarantine_report)s, %(weekly_quarantine_report)s, %(monthly_quarantine_report)s, %(custom_spam_score)s, %(custom_spam_highscore)s, %(skip_scan)s, %(last_login)s, %(date_joined)s) RETURNING id", (vals))
        if 'domaintable' in tables:
            user_id = pgsql_cursor.fetchone()[0]
            pgsql_cursor.execute("SELECT id from domains_domain WHERE name='{0}' LIMIT 1".format(user['username'] if not '@' in user['username'] else user['username'].split('@')[1]))
            data = pgsql_cursor.fetchone()
            if not data:
                print('[{0}%] :: Processing user {1} :: No domains found'.format(round((count/total) * 100, 2), user['username']))
                errors.append("No domains could be found for user {0}. Please check".format(user['username']))
                continue
            primary_domain_id = data[0]
            pgsql_cursor.execute("INSERT INTO core_user_domains (user_id, domain_id) VALUES(%s, %s)", (user_id, primary_domain_id))
            domains_cursor = mysql_conn2.cursor(dictionary=True)
            domains_cursor.execute("SELECT * FROM domaintable WHERE domainadmin='{0}'".format(user['username'] if not '@' in user['username'] else user['username'].split('@')[1]))
            for entry in domains_cursor:
                pgsql_cursor.execute("SELECT id FROM domains_domain WHERE name='{0}'".format(entry['domainname']))
                domain_id = pgsql_cursor.fetchone()[0]
                if not domain_id == primary_domain_id:
                    pgsql_cursor.execute("INSERT INTO core_user_domains (user_id, domain_id) VALUES(%s, %s)", (user_id, domain_id))
        pgsql_conn.commit()
        count += 1
    domains_cursor.close()
    pgsql_cursor.close()
    mysql_cursor.close()
    print('Checking relationship between users and domains seen from the domain as the starting point...')
    pgsql_cursor = pgsql_conn.cursor()
    
    pgsql_cursor.execute("SELECT domain_id FROM core_user_domains")
    dlist = ""
    for domain in pgsql_cursor:
        dlist += "'{0}', ".format(domain[0])
    pgsql_cursor.close()
    pgsql_cursor = pgsql_conn.cursor()
    pgsql_cursor.execute("SELECT id, name FROM domains_domain WHERE id NOT IN ({0})".format(dlist[:-1]))
    for domain in pgsql_cursor:
        errors.append('No users connected to domain {0}. Please check'.format(domain[1]))

    mysql_conn2.close()
    mysql_conn.close()

    pgsql_cursor.close()
    pgsql_conn.close()
    stop_time = datetime.datetime.now()
    start_ts = time.mktime(start_time.timetuple())
    stop_ts = time.mktime(stop_time.timetuple())
    print('Started {0} and stopped {1}. Total time spent is {2} minutes'.format(start_time, stop_time, int(start_ts-stop_ts) / 60))
    if errors:
        print('There were errors or warnings during execution')
        for error in errors:
            print(error)
