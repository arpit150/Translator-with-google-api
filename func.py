import re 
import psycopg2
from englisttohindi.englisttohindi import EngtoHindi 
  


countHindi =  {'0': '०', '1': '१', '2': '२', '3': '३', '4': '४', '5': '५', '6': '६', '7': '७', '8': '८', '9': '९'}
countBeng =   {'0': '০', '1': '১', '2': '২', '3': '৩', '4': '৪', '5': '৫', '6': '৬', '7': '৭', '8': '৮', '9': '৯'}
countGujrati= {'0': '૦', '1': '૧', '2': '૨', '3': '૩', '4': '૪', '5': '૫', '6': '૬', '7': '૭', '8': '૮', '9': '૯'}
countKannada ={'0': '೦', '1': '೧', '2': '೨', '3': '೩', '4': '೪', '5': '೫', '6': '೬', '7': '೭', '8': '೮', '9': '೯'}
countMal =    {'0': '൦', '1': '൧', '2': '൨', '3': '൩', '4': '൪', '5': '൫', '6': '൬', '7': '൭', '8': '൮', '9': '൯'}
countMarathi = {'0': '०', '1': '१', '2': '२', '3': '३', '4': '४', '5': '५', '6': '६', '7': '७', '8': '८', '9': '९'}
countTamil =  {'0': '௦', '1': '௧', '2': '௨', '3': '௩', '4': '௪', '5': '௫', '6': '௬', '7': '௭', '8': '௮', '9': '௯'}
countTelagu = {'0': '౦', '1': '౧', '2': '౨', '3': 'త్రీ', '4': '౪', '5': '౫', '6': '౬', '7': '౭', '8': '౮', '9': '౯'}
countPunjabi ={'0': '੦', '1': '੧', '2': '੨', '3': '੩', '4': '੪', '5': '੫', '6': '੬', '7': '੭', '8': '੮', '9': '੯'}
countNepali = {'0': '०', '1': '१', '2': '२', '3': '३', '4': '४', '5': '५', '6': '६', '7': '७', '8': '८', '9': '९'}
countOriya =  {'0': '୦', '1': '୧', '2': '୨', '3': '୩', '4': '୪', '5': '୫', '6': '୬', '7': '୭', '8': '୮', '9': '୯'}

lang_dict ={
            'hi':countHindi,
            'bn':countBeng,
            'gu':countGujrati,
            'kn':countKannada,
            'ml':countMal,
            'ta':countTamil,
            'te':countTelagu,
            'pa':countPunjabi,
            'ne':countNepali,
            'or':countOriya,
            'mr':countMarathi
            }

conn = psycopg2.connect("dbname='python_trans' user='postgres' host='localhost' password='postgres'")
cur = conn.cursor()
def check_type(word):
    emp_dict = {}
    emp_dict['type'] ='false'
    type = 'ad'
    emp_dict['word'] =word
    if bool(re.match('^[A-Z]+$', word)):
        # print('.'.join(list(word)))
        emp_dict['type'] = 'abbr'
    elif bool(re.search('[\\\/\-]+',word)) and bool(re.search('[\d]',word)):
        emp_dict['type']  = 'address'

    elif bool(re.search('(\.in|\.com|\.co\.in|\.org|\.gov\.in|\.net|\.edu)',word,re.IGNORECASE)):
        emp_dict['type']  = 'web'

    # elif word.isalnum and bool(re.search('[\d]',word)):
    elif re.search("[a-zA-Z]+", word) and re.search("[\d]+", word) and not  re.search("[$&\[+\],:;=?@#|\'<>\.\^*()\-%!]+", word) :
        emp_dict['type']  = 'alphanum'

    elif word.isdigit():
        emp_dict['type']  = 'numeric'

    elif re.search("[$&\[+\],:;=?@#|\'<>\.\^*()\-%!]+", word):
        emp_dict['type']  = 'special'

    elif re.search("^[a-zA-Z ]+$", word):
        emp_dict['type']  = 'normal'

    return emp_dict

# print(check_type('Arkkamala'))

def createTable(conn,tableName):
    try:
        cur = conn.cursor()
        drop_query =f"DROP TABLE IF EXISTS data.{tableName} CASCADE"
        cur.execute(drop_query)
        conn.commit()


        create_query = f'''CREATE TABLE data.{tableName} (
                    id text,
                    input text,
                    output text,
                    remark text
                    )'''
        cur.execute(create_query)
        conn.commit()
        index_name = str(tableName)+'_index'
        index_query = f'''CREATE INDEX {index_name}
                        ON data.{tableName} (input)'''
        cur.execute(index_query)
        conn.commit()

        #trigger function query
#         -- FUNCTION: data.update_function()

# -- DROP FUNCTION data.update_function();

# CREATE FUNCTION data.update_function_()
#     RETURNS trigger
#     LANGUAGE 'plpgsql'
#     COST 100
#     VOLATILE NOT LEAKPROOF 
# AS $BODY$

# BEGIN
# 	IF (NEW.input ~ '^[A-Z]+$')
# 		THEN new.remark = 'abbr';
# 	ELSEIF (NEW.input ~ '[\\\/\-]+') and (NEW.input ~ '[\d]')
# 		THEN 
# 		new.remark ='address';
# 		new.output = new.input;
# 	ELSEIF (NEW.input ~* '(\.in|\.com|\.co\.in|\.org|\.gov\.in|\.net|\.edu)')
# 		THEN 
# 		new.remark ='web';
# 		new.output = new.input;
		
# 	ELSEIF ((NEW.input ~ '[a-zA-Z]+') and (NEW.input ~ '[\d]+'))
# 			and (NEW.input !~ '[$&\[+\],:;=?@#|''<>.\^*()\-%!]+')
# 		THEN new.remark ='alphanum';
# 	ELSEIF (NEW.input ~ '^[0-9]+$')
# 		THEN 
# 		new.remark ='numeric';
# 		new.output = new.input;
# 	ELSEIF (NEW.input ~ '[\\\/$&\[+\],:;=?@#|''<>\.\^*()\-%!]+')
# 		THEN new.remark ='special';
# 	ELSEIF (NEW.input ~ '^[a-zA-Z ]+$')
# 		THEN new.remark ='normal';
# 	ELSE
# 		 new.remark = 'extra';
# 	END IF;
# 	return new;
	
# END;
# $BODY$;

# ALTER FUNCTION data.update_function()
#     OWNER TO postgres;

        
        trigger_query = f"create trigger update_trigger before insert on data.{tableName} for each row execute procedure data.update_function_();"
        cur.execute(trigger_query)
        conn.commit()
        # print('sasad')
    except psycopg2.OperationalError as e:
        print('Unable to connect!\n{0}').format(e)
        sys.exit()

def insert_db (conn,tableName,id,input,output,remark,):
    try:
        cur = conn.cursor()       
        insert_query = f"INSERT INTO data.{tableName}(id, input, output, remark) VALUES(%s,%s,%s,%s)" 
        # print(insert_query)
        cur.execute(insert_query,(id,input,output,remark))
        conn.commit()

        return 'success'
    except:
        return 'error'

def arrayToString(conn,offset,tablename,type_,limitoffset,delimiter):
    cur=conn.cursor()
    q = f"SELECT array_to_string(Array(SELECT input FROM data.{tablename} where remark='{type_}' and output=''     order by(id) OFFSET {offset} limit {limitoffset} ),'{delimiter}')"
    # print(q)
    cur.execute(q)
    result = cur.fetchone()
    return result[0]


def type_count(tableName,type_):

    count_normal_query = f"SELECT count(id) FROM data.{tableName} where remark='{type_}' and output ='' "
    cur.execute(count_normal_query)
    result = cur.fetchone()
    count_normal =result[0]
    print(count_normal_query)
    return count_normal

def alphanum_break(word):
    check_word = 'alpha'
    a=''
    for i in word:
        if i.isalpha():
            current_word = 'alpha'
        else:
            current_word = 'extra'
        if check_word ==current_word:
            a = a+i
        else:
            a=a+'.'
            a=a+i
        check_word=current_word
    return a.strip('.')

def digit_convert(word,lang):
    str_ =''
    rdict = dict((v,k) for k,v in lang_dict[lang].items())

    for i in word:
        if i in rdict.keys():
            str_ =str_+ str(rdict[i])
        else:
            str_=str_+i
    return str_.replace('.','')
    # print(rdict.keys())

def special_break(word):
    check = True
    check_word = 'special'
    a=''
    for i in word:
        if i.isalpha():
            current_word = 'special'
        else:
            current_word = 'extra'
        if check_word ==current_word:
            a = a+i
        else:
            a=a+'τ'
            a=a+i
        check_word=current_word
    return a.strip('τ')

def speical_digit_convert(word,lang):
    str_ =''
    rdict = dict((v,k) for k,v in lang_dict[lang].items())

    for i in word:
        if i in rdict.keys():
            str_ =str_+ str(rdict[i])
        else:
            str_=str_+i
    return str_


def replace_(_str,to_replace,_with):
    return _str.replace(to_replace,_with)



def convertTohindi(message):
    res = EngtoHindi(message) 
    return res.convert