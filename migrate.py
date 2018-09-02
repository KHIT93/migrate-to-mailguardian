import mysql.connector
import psycopg2
import argparse
import json
import uuid

parser = argparse.ArgumentParser()

parser.add_argument('--mailwatch-config', help='The path to conf.php for your MailWatch installation')
parser.add_argument('--mailguardian-config', help='The path to mailguardian-env.json for your MailGuardian installation')

args = parser.parse_args()

mysql_config = {}
pgsql_config = {}
src = {}
mysql_conn = None
pgsql_conn = None

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
    except mysql.connector.Error as e:
        print(e)
        exit()

    print('Successfully connected to source MySQL database for MailWatch' + chr(13))
    
    try:
        pgsql_conn = psycopg2.connect(dbname=pgsql_config['name'], user=pgsql_config['username'], password=pgsql_config['password'], host=pgsql_config['host'], port=pgsql_config['port'], sslmode=pgsql_config['sslmode'])
    except psycopg2.OperationalError as e:
        print(e)
        exit()

    print('Successfully connected to the destination PostgreSQL database for MailGuardian' + chr(13))
    
    print('Successfully connected to source and destination datastorage' + chr(13))
    pgsql_cursor = pgsql_conn.cursor()
    mysql_cursor = mysql_conn.cursor()
    print('Getting list of tables from source database' + chr(13))
    mysql_cursor.execute("SHOW TABLES")
    tables = []
    for row in mysql_cursor.fetchall():
        tables.append(row[0])

    mysql_cursor = mysql_conn.cursor(dictionary=True)
    print('Counting maillog entries to process' + chr(13))
    mysql_cursor.execute("SELECT count(id) as id__count FROM maillog")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    print('Collecting maillog entries to process' + chr(13))
    mysql_cursor.execute("SELECT * FROM maillog")
    for message in mysql_cursor:
        print('[{0}%] :: Processing message {1}'.format((int)(count/total), message['id']))
        vals = {
            'id': uuid.uuid4(),
            'from_address': message['from_address'],
            'from_domain': message['from_domain'],
            'to_address': message['to_address'],
            'to_domain': message['to_domain'],
            'subject': message['subject'] if message['subject'] else "",
            'client_ip': message['clientip'],
            'mailscanner_hostname': message['hostname'],
            'spam_score': message['sascore'],
            'mcp_score': message['mcpsascore'],
            'timestamp': message['timestamp'],
            'date': message['date'],
            'size': message['size'],
            'token': message['token'],
            'mailq_id': message['id'],
            'whitelisted': True if message['spamwhitelisted'] == 1 else False,
            'blacklisted': True if message['spamblacklisted'] == 1 else False,
            'is_spam': True if message['issaspam'] == 1 or message['isspam'] == 1 or message['ishighspam'] == 1 else False,
            'is_mcp': True if message['issamcp'] == 1 or message['ismcp'] == 1 or message['ishighmcp'] == 1 else False,
            'is_rbl_listed': True if message['isrblspam'] == 1 else False,
            'quarantined': True if message['quarantined'] == 1 else False,
            'infected': True if message['virusinfected'] == 1 or message['nameinfected'] == 1 or message['otherinfected'] == 1 else False,
            'released': True if message['released'] == 1 else False
        }
        pgsql_cursor.execute("INSERT INTO mail_message (id, from_address, from_domain, to_address, to_domain, subject, client_ip, mailscanner_hostname, spam_score, timestamp, token, whitelisted, blacklisted, is_spam, is_rbl_listed, quarantined, infected, size, mailq_id, is_mcp, mcp_score, date, released) VALUES('{id}', '{from_address}', '{from_domain}', '{to_address}', '{to_domain}', '{subject}', '{client_ip}', '{mailscanner_hostname}', {spam_score}, '{timestamp}', '{token}', {whitelisted}, {blacklisted}, {is_spam}, {is_rbl_listed}, {quarantined}, {infected}, {size}, '{mailq_id}', {is_mcp}, {mcp_score}, '{date}', {released}) RETURNING id".format(**vals))
        message_id = pgsql_cursor.fetchone()[0]
        vals = {
            'id': uuid.uuid4(),
            'contents': message['headers'],
            'message_id': message_id
        }
        pgsql_cursor.execute("INSERT INTO mail_headers (id, contents, message_id) VALUES ('{id}', '{contents}', '{message_id}')".format(**vals))
        vals = {
            'id': uuid.uuid4(),
            'contents': message['report'],
            'message_id': message_id
        }
        pgsql_cursor.execute("INSERT INTO mail_mailscannerreport (id, contents, message_id) VALUES ('{id}', '{contents}', '{message_id}')".format(**vals))
        vals = {
            'id': uuid.uuid4(),
            'contents': message['mcpreport'],
            'message_id': message_id
        }
        pgsql_cursor.execute("INSERT INTO mail_mcpreport (id, contents, message_id) VALUES ('{id}', '{contents}', '{message_id}')".format(**vals))
        vals = {
            'id': uuid.uuid4(),
            'contents': message['rblspamreport'],
            'message_id': message_id
        }
        pgsql_cursor.execute("INSERT INTO mail_rblreport (id, contents, message_id) VALUES ('{id}', '{contents}', '{message_id}')".format(**vals))
        vals = {
            'id': uuid.uuid4(),
            'contents': message['report'],
            'message_id': message_id
        }
        pgsql_cursor.execute("INSERT INTO mail_spamreport (id, contents, message_id) VALUES ('{id}', '{contents}', '{message_id}')".format(**vals))
        mysql_cursor.execute("SELECT * FROM mtalog WHERE msg_id={0}".format(message_id))
        for entry in mysql_cursor.fetchall():
            vals = {
                'id': uuid.uuid4(),
                'timestamp': entry['timestamp'],
                'message_id': message_id,
                'transport_host': entry['host'],
                'transport_type': entry['type'],
                'relay_host': entry['relay'],
                'dsn': entry['dsn'],
                'dsn_message': entry['status'],
                'delay': entry['delay']
            }
            pgsql_cursor.execute("INSERT INTO mail_transportlog (id, timestamp, message_id, transport_host, transport_type, relay_host, dsn, dsn_message, delay) VALUES('{id}', '{timestamp}', '{message_id}', '{transport_host}', '{transport_type}', '{relay_host}', '{dsn}', '{dsn_message}', '{delay}')".format(**vals))
        count += 1
    mysql_cursor.execute("SELECT count(id) as id__count FROM blacklist")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    mysql_cursor.execute("SELECT * FROM blacklist")
    for entry in mysql_cursor:
        print('[{0}%] :: Processing blacklist {1}'.format((int)(count/total), message['id']))
        vals = {
            'id': uuid.uuid4(),
            'from_address': '*' if entry['from_address'] == 'default' else entry['from_address'],
            'to_address': '*' if entry['to_address'] == 'default' else entry['to_address'],
            'to_domain': (entry['to_domain'] if entry['to_domain'] != 'default' else '*') if entry['to_domain'] else '',
            'listing_type': 'blacklisted'
        }
        pgsql_cursor.execute("INSERT INTO list_entries (id, from_address, to_address, to_domain, listing_type) VALUES('{id}', '{from_address}', '{to_address}', '{to_domain}', '{listing_type}')".format(**vals))
        count += 1

    mysql_cursor.execute("SELECT count(id) as id__count FROM blacklist")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    mysql_cursor.execute("SELECT * FROM whitelist")
    for entry in mysql_cursor:
        print('[{0}%] :: Processing whitelist {1}'.format((int)(count/total), message['id']))
        vals = {
            'id': uuid.uuid4(),
            'from_address': '*' if entry['from_address'] == 'default' else entry['from_address'],
            'to_address': '*' if entry['to_address'] == 'default' else entry['to_address'],
            'to_domain': (entry['to_domain'] if entry['to_domain'] != 'default' else '*') if entry['to_domain'] else '',
            'listing_type': 'whitelisted'
        }
        pgsql_cursor.execute("INSERT INTO list_entries (id, from_address, to_address, to_domain, listing_type) VALUES('{id}', '{from_address}', '{to_address}', '{to_domain}', '{listing_type}')".format(**vals))
        count += 1
    
    if 'smtpaccess' in tables:
        mysql_cursor.execute("SELECT count(id) as id__count FROM smtpaccess")
        total = mysql_cursor.fetchone()['id__count']
        count = 0
        mysql_cursor.execute("SELECT * FROM smtpaccess")
        for entry in mysql_cursor:
            print('[{0}%] :: Processing SMTP relay {1}'.format((int)(count/total), message['id']))
            vals = {
                'id': uuid.uuid4(),
                'ip_address': entry['smtpvalue'],
                'comment': entry['comment'],
                'active': True,
                'hostname': entry['smtpvalue']
            }
            pgsql_cursor.execute("INSERT INTO mail_smtprelay (id, ip_address, comment, active, hostname) VALUES('{id}', '{ip_address}', '{comment}', {active}, '{hostname}')".format(**vals))
            count += 1
    if 'domaintable' in tables:
        mysql_cursor.execute("SELECT count(id) as id__count FROM domaintable")
        total = mysql_cursor.fetchone()['id__count']
        count = 0
        mysql_cursor.execute("SELECT * FROM domaintable")
        for entry in mysql_cursor:
            print('[{0}%] :: Processing domain {1}'.format((int)(count/total), message['id']))
            vals = {
                'id': uuid.uuid4(),
                'name': entry['domainname'],
                'destination': entry['relaymap'],
                'relay_type': entry['relaytype'],
                'created_timestamp': entry['createdts'],
                'updated_timestamp': entry['createdts'],
                'active': True,
                'catchall': True,
                'allowed_accounts': entry['accountno']
            }
            pgsql_cursor.execute("INSERT INTO doamins_domain (id, name, destination, relay_type, created_timestamp, updated_timestamp, active, catchall, allowed_accounts) VALUES('{id}', '{name}', '{destination}', '{relay_type}', '{created_timestamp}', '{updated_timestamp}', {active}, {catchall}, {allowed_accounts})".format(**vals))
    mysql_cursor.execute("SELECT count(id) as id__count FROM users")
    total = mysql_cursor.fetchone()['id__count']
    count = 0
    mysql_cursor.execute("SELECT * FROM users")
    for user in mysql_cursor:
        print('[{0}%] :: Processing user {1}'.format((int)(count/total), message['id']))
        vals = {
            'id': uuid.uuid4(),
            'email': user['username'] if '@' in user['username'] else 'admin@' . user['username'],
            'first_name': user['fullname'],
            'is_domain_admin': True if user['type'] == 'D' else False,
            'is_staff': True if user['type'] == 'A' else False,
            'is_superuser': True if user['type'] == 'A' else False,
            'is_active': True,
            'daily_quarantine_report': True if user['quarantine_report'] == 1 else False,
            'custom_spam_score': 5 if user['spamscore'] == 0 else user['spamscore'],
            'custom_highspam_score': 15 if user['highspamscore'] == 0 else user['highspamscore'],
            'skip_scan': True if user['noscan'] == 1 else False,
            'last_login': None if user['last_login'] == -1 else user['last_login']
        }
        pgsql_cursor.execute("INSERT INTO core_user (id, email, first_name, is_domain_admin, is_staff, is_superuser, is_active, daily_quarantine_report, custom_spam_score, custom_highspam_score, skip_scan, last_login) VALUES('{id}', '{email}', '{first_name}', {is_domain_admin}, {is_staff}, {is_superuser}, {is_active}, {daily_quarantine_report}, {custom_spam_score}, {custom_highspam_score}, {skip_scan}, '{last_login}') RETURNING id".format(**vals))
        if 'domaintable' in tables:
            user_id = pgsql_cursor.fetchone()[0]
            pgsql_cursor.execute("SELECT id from domains_domain WHERE name='{0}' LIMIT 1".format(user['username'] if not '@' in user['username'] else user['username'].split('@')[1]))
            domain_id = pgsql_cursor.fetchone()[0]
            pgsql_cursor.execute("INSERT INTO core_user_domains (user_id, domain_id) VALUES('{0}', '{1}')".format(user_id, domain_id))
            mysql_cursor.execute("SELECT * FROM domaintable WHERE domainadmin='{0}'".format(user['username'] if not '@' in user['username'] else user['username'].split('@')[1]))
            for entry in mysql_cursor.fetchall():
                vals = {
                    'id': uuid.uuid4(),
                    'name': entry['domainname'],
                    'destination': entry['relaymap'],
                    'relay_type': entry['relaytype'],
                    'created_timestamp': entry['createdts'],
                    'updated_timestamp': entry['createdts'],
                    'active': True,
                    'catchall': True,
                    'allowed_accounts': entry['accountno']
                }
                pgsql_cursor.execute("INSERT INTO doamins_domain (id, name, destination, relay_type, created_timestamp, updated_timestamp, active, catchall, allowed_accounts) VALUES('{id}', '{name}', '{destination}', '{relay_type}', '{created_timestamp}', '{updated_timestamp}', {active}, {catchall}, {allowed_accounts})".format(**vals))
        count += 1

    mysql_conn.close()

    pgsql_cursor.close()
    pgsql_conn.close()