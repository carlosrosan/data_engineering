import requests
import pandas as pd
import re
import datetime
from datetime import datetime, date, timedelta
import os
import json
import glob
import logging

# Log Config

logging.basicConfig(filename='app_get.log', level=logging.INFO,format='%(asctime)s ''-'' %(funcName)s ''Msg:'' %(message)s '' near line'' %(lineno)d', datefmt='%d/%m/%Y %I:%M:%S %p')

# Main Class for quering data

class API_Query:
    
    def __init__(self):
        try:
            self.config_file = self.config_json()
            self.query_count = 0
        except Exception as E:
            logging.warning(E)
            exit()
        pass

        # API Query config

    def config_json(self): 
        try:
            with open('./config.json') as f:
                _config_file = json.load(f)
            return _config_file
        except Exception as E:
            logging.warning(E)
            return 

        # Authentication
    
    def API_Get_Token(self):

        try:    
            config_file = self.config_file

            headers = {config_file['config']['API_keyName']: config_file['config']['API_key']}
            Authorization = requests.get(config_file['config']['API_url'] + "/authenticate?user="+ config_file['config']['API_user'], headers=headers)
                
            pattern = '{"status":"ok","token":"(.*?)","message":"Authentication success."}'
            token = re.search(pattern, Authorization.text).group(1)

            self.query_count += 1

            return token

        except Exception as E:
            logging.warning(E)
            return 
    
    def API_Headers(self):

        try:
            config_file = self.config_file
            headers = {config_file['config']['API_keyName']: config_file['config']['API_key'],"Authorization":str("Bearer "+ self.API_Get_Token())}
            return headers

        except Exception as E:
            logging.warning(E)
            return 

        # Get IDs of cases to query

    def CSV_Case_ids(self, _case_csv_name):
        
        try:
            data_cases = pd.read_csv(_case_csv_name, encoding='UTF-16', sep=';')
            _case_ids = data_cases.id.values

            return _case_ids

        except Exception as E:
            logging.warning(E)
            return 
        
        # Get Cases Users (Contacts) to query

    def API_Query_Cases_Contacts_CF_Tags(self, _url, desde, hasta):
        
        try:
            headers = self.API_Headers()
            url = self.config_file['config']['API_url']
            cases_filter_by = self.config_file['download_extract']['cases']['filter_by']
            fields_casos = ','.join(str(elem) for elem in self.config_file['download_extract']['cases']['columnas_casos'])
            fields_contactos = ','.join(str(elem) for elem in self.config_file['download_extract']['cases']['columnas_contactos'])

            pagina = 1
            paginas = range(1,5) #La cantidad de páginas máxima puede ampliarse
            _Result = " "
            stop_error = '{"error":"Cases not found.","code":400}'
            _dataContainer = []
            _dataContainer_CF = []
            _dataContainer_Tags = []
            Value_Error=0
            
            if (_Result != stop_error) | (pagina != paginas[-1]) is True:

                queryClose = False

                while  queryClose ==False:

                    if _url is None:                    
                        Query = requests.get(url + "/cases?sort=asc&sort_field=last_update&limit=1000&page="+ str(pagina) +"&filtering=[%7B'field':'case." + cases_filter_by + "','operator':'GREATER EQUAL','value':'"+ str(desde) +"'%7D,%7B'field':'case." + cases_filter_by + "','operator':'LOWER','value':'"+ str(hasta) +"'%7D]&fields=" + fields_casos, headers=headers)
                        
                        self.query_count += 1

                        _datos = Query.text.replace('https:','').replace('http:','').replace('.com','_url')
                        
                        _datos = json.loads(_datos)

                        try:
                            _dataLoad =  json.dumps(_datos['data'])
                            _df = pd.read_json(_dataLoad)

                        except Exception as E:
                            logging.info(_datos)
                            logging.warning(E)
                            break

                        try:
                            _dataContainer.append(_df)

                            for p in range(0,len(_df)):
                                _dfCustomFields = pd.DataFrame(_df.custom_fields[p])
                                _dfCustomFields['case_id'] = _df.id[p]
                                _dataContainer_CF.append(pd.DataFrame(_dfCustomFields).rename(columns={"field": "name"}))

                                _dfTags = pd.DataFrame(_df.tags[p], columns=['tag'])
                                _dfTags['case_id'] = _df.id[p]
                                _dataContainer_Tags.append(pd.DataFrame(_dfTags).rename(columns={"field": "name"}))

                            _nextURL=_datos['paging']['next']
            
                            if _nextURL=='' or Value_Error==1:
                                queryClose = True

                            else:
                                pagina+=1

                        except Exception as E:
                            logging.warning(E)
                            pass

                                
                _dfCompleto_Casos = pd.concat(_dataContainer)
                _dfCompleto_CustomFields = pd.concat(_dataContainer_CF)
                _dfCompleto_Tags = pd.concat(_dataContainer_Tags)

            _dfCompleto_Casos.status = _dfCompleto_Casos.status.str.replace('\n',' ')
            _dfCompleto_Casos.subject = _dfCompleto_Casos.subject.str.replace('\n',' ')

            _dataContainer = []

            for i in range(0, len(_dfCompleto_Casos)):

                try:
                    
                    contactid = _dfCompleto_Casos.contact_id[i]
                    Query = requests.get(url + "/contacts/"+ str(contactid) +"?fields="+ fields_contactos, headers=headers)
                    
                    self.query_count += 1
                    
                    _datos = json.loads(Query.text)
                    _dataLoad =  json.dumps(_datos)
                    _df = pd.read_json(_dataLoad)
                    _dataContainer.append(_df)
                    
                except Exception as E:
                    logging.info('No se encontraron datos para el contacto ' + str(contactid) + ' mencionado en el caso '+ str(_dfCompleto_Casos.id[i]))

            _dfCompleto_contactos = pd.concat(_dataContainer)
                
            return _dfCompleto_Casos, _dfCompleto_contactos, _dfCompleto_CustomFields, _dfCompleto_Tags

        except Exception as E:
           logging.info('La siguiente excepción se asume como ausencia de casos en el rango horario consultado.')
           logging.warning(E)
           return 
        
        # Get Interactions (activities) of each Case

    def API_Query_activities(self, _case_csv_name, _case_ids, _desde, _hasta):

        try:
            
            global hora
            url = self.config_file['config']['API_url']
            activities_filter_by = self.config_file['download_extract']['activities']['filter_by']
            fields_activities = ','.join(str(elem) for elem in self.config_file['download_extract']['activities']['columnas'])
            
            headers = self.API_Headers()

            _dfCompleto = pd.DataFrame(columns = self.config_file['download_extract']['activities']['columnas'])

            for j in range(0,len(_case_ids)-1):

                try:
                    case_id_out = _case_ids[j]

                    Query_activity = requests.get(url + "/cases/"+str(case_id_out)+ 
                    "/activities?&limit=1000&page=1&filtering=[%7B'field':'case." + activities_filter_by + "','operator':'GREATER EQUAL','value':'"+ 
                    str(_desde) +"'%7D,%7B'field':'case." + activities_filter_by + "','operator':'LOWER','value':'"+ str(_hasta) +"'%7D]&fields=" +  fields_activities, headers=headers)

                    self.query_count += 1
                    
                    _datos = json.loads(Query_activity.text)
                    
                    _df_activities_case = pd.DataFrame.from_dict(_datos[0], orient='index').T

                    for i in range(1,len(_datos)-1):

                        _df_activities_case = _df_activities_case.append(pd.DataFrame.from_dict(_datos[i], orient='index').T)

                    _df_activities_case['case_id'] = round(case_id_out, 0)

                    _dfCompleto = _dfCompleto.append(_df_activities_case)

                except Exception as E:
                    logging.warning(E)
                    pass

            
            _dfCompleto.content = _dfCompleto.content.str.replace('\n',' ')

            return _dfCompleto

        except Exception as E:
            logging.warning(E)
            return 
        
        # Get Types of Cases (Semi static dimmension)

    def API_Query_CaseTypes(self):

        url = self.config_file['config']['API_url']
        headers = self.API_Headers()
        try:
                        
            Query = requests.get(url + "/cases/types?fields=id,name,parent_id", headers=headers)
            
            self.query_count += 1

            _datos = Query.text
            
            _datos = json.loads(_datos)

            try:
                _dataLoad =  json.dumps(_datos['data'])
                _dfCaseTypes = pd.read_json(_dataLoad)

            except Exception as E:
                logging.warning(E)

            return _dfCaseTypes

        except Exception as E:
            logging.warning(E)
            return 

        # Get User(Contacts) Queues 

    def API_Query_Users_Queues(self):

        url = self.config_file['config']['API_url']
        headers = self.API_Headers()
        _dataContainer = []
        try:
                        
            Query = requests.get(url + "/users?fields=id,name,nick,full_name,queues_ids", headers=headers)
            
            self.query_count += 1

            _datos = Query.text
            
            _datos = json.loads(_datos)

            try:
                _dataLoad =  json.dumps(_datos['data'])
                _dfUsers = pd.read_json(_dataLoad)

                try:
                    for k in range(0,len(_dfUsers)):
                        _dfUser_Queues = pd.DataFrame({'queue_id':_dfUsers['queues_ids'][k]})
                        _dfUser_Queues['user_id'] = _dfUsers['id'][k]
                        _dataContainer.append(pd.DataFrame(_dfUser_Queues))
                        
                    _dfCompleto_User_Queues = pd.concat(_dataContainer)
                    _dfUsers.drop('queues_ids', inplace=True, axis=1)


                except Exception as E:
                    logging.warning(E)
                

            except Exception as E:
                logging.warning(E)

            return _dfUsers, _dfCompleto_User_Queues

        except Exception as E:
            logging.warning(E)
            return 

        # Get Queues (Semi static dimmension)

    def API_Query_Queues(self):

        url = self.config_file['config']['API_url']
        headers = self.API_Headers()
        try:
                        
            Query = requests.get(url + "/queues?fields=id,name", headers=headers)
            
            self.query_count += 1

            _datos = Query.text
            
            _datos = json.loads(_datos)

            try:
                _dataLoad =  json.dumps(_datos['data'])
                _dfQueues = pd.read_json(_dataLoad)

            except Exception as E:
                logging.warning(E)

            return _dfQueues

        except Exception as E:
            logging.warning(E)
            return 

        # Execute complete Query

    def PY_Query_Exec(self): 

        begin_time = datetime.now()

        try:

            cases_path = self.config_file['download_extract']['cases']['path']
            cases_out_path = self.config_file['download_extract']['cases']['cases_out_path']
            cases_filename = self.config_file['download_extract']['cases']['filename']

            contacts_path = self.config_file['download_extract']['cases']['contacts_path']
            contacts_out_path = self.config_file['download_extract']['cases']['contacts_out_path']
            contacts_filename = self.config_file['download_extract']['cases']['contacts_filename']

            cf_path = self.config_file['download_extract']['cases']['cf_path']
            cf_out_path = self.config_file['download_extract']['cases']['cf_out_path']
            cf_filename = self.config_file['download_extract']['cases']['cf_filename']

            tags_path = self.config_file['download_extract']['cases']['tags_path']
            tags_out_path = self.config_file['download_extract']['cases']['tags_out_path']
            tags_filename = self.config_file['download_extract']['cases']['tags_filename']

            types_path = self.config_file['download_extract']['types']['path']
            types_out_path = self.config_file['download_extract']['types']['types_out_path']
            types_filename = self.config_file['download_extract']['types']['filename']

            users_path = self.config_file['download_extract']['users']['path']
            users_out_path = self.config_file['download_extract']['users']['users_out_path']
            users_filename = self.config_file['download_extract']['users']['filename']

            user_queues_path = self.config_file['download_extract']['user_queues']['path']
            user_queues_out_path = self.config_file['download_extract']['user_queues']['out_path']
            user_queues_filename = self.config_file['download_extract']['user_queues']['filename']

            activities_path = self.config_file['download_extract']['activities']['path']
            activities_out_path = self.config_file['download_extract']['activities']['out_path']
            activities_filename = self.config_file['download_extract']['activities']['filename']
            
            if not os.path.exists(cases_path):
                os.makedirs(cases_path)

            if not os.path.exists(cases_out_path):
                os.makedirs(cases_out_path)

            if not os.path.exists(contacts_path):
                os.makedirs(contacts_path)

            if not os.path.exists(contacts_out_path):
                os.makedirs(contacts_out_path)

            if not os.path.exists(cf_path):
                os.makedirs(cf_path)

            if not os.path.exists(cf_out_path):
                os.makedirs(cf_out_path)

            if not os.path.exists(tags_path):
                os.makedirs(tags_path)

            if not os.path.exists(tags_out_path):
                os.makedirs(tags_out_path)

            if not os.path.exists(types_path):
                os.makedirs(types_path)

            if not os.path.exists(types_out_path):
                os.makedirs(types_out_path)

            if not os.path.exists(users_path):
                os.makedirs(users_path)

            if not os.path.exists(users_out_path):
                os.makedirs(users_out_path)

            if not os.path.exists(user_queues_path):
                os.makedirs(user_queues_path)

            if not os.path.exists(user_queues_out_path):
                os.makedirs(user_queues_out_path)

            if not os.path.exists(activities_path):
                os.makedirs(activities_path)

            if not os.path.exists(activities_out_path):
                os.makedirs(activities_out_path)

        except Exception as E:
            logging.warning(E)

        i = 0

        fecha_json = datetime.strptime(self.config_file['download_extract']['cases']['last_date'], '%Y%m%d').strftime('%Y-%m-%d')
        hora_json = datetime.strptime(self.config_file['download_extract']['cases']['last_time'], '%H%M').strftime('%H:%M')
        max_days_query =  self.config_file['config']['max_days_query'] 

        hora_input_json = fecha_json + ' ' + hora_json
        hora_utc_input = datetime.strptime(hora_input_json, "%Y-%m-%d %H:%M")
        hora_i = hora_utc_input        
        
        # Se realizaran consultas de rangos de 1 hora hasta que la hora de la consulta coincida con la hora UTC actual:
        
        if datetime.now() - hora_i - timedelta(hours = 3) < timedelta(days=max_days_query):
            
            while datetime.now() - hora_i - timedelta(hours = 3) > timedelta(hours = 1):

                hora_i = hora_utc_input + timedelta(days = 0, hours=i, minutes=0)

                desde_utc = hora_i.strftime("%Y-%m-%d %H")
                desde = '{}:00:00'.format(desde_utc)

                logging.info('Consultando 1hr de Casos, Contactos, Custom Fields, Tags, Tipos y Actividades desde '+desde)

                hasta_utc_g = hora_i + timedelta(hours=1)
                hasta_utc = hasta_utc_g.strftime("%Y-%m-%d %H")
                hasta = '{}:00:00'.format(hasta_utc)

                try:

                    datos_out_casos, datos_out_contactos, datos_out_CF, datos_out_tags = self.API_Query_Cases_Contacts_CF_Tags(None, desde, hasta)
                    
                    cases_csv_name = cases_path + cases_filename +'{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H")))

                    datos_out_casos.to_csv(cases_csv_name, index=False, encoding='UTF-16', sep=';')

                    self.config_file['download_extract']['cases']['last_date'] = hora_i.strftime("%Y%m%d")
                    self.config_file['download_extract']['cases']['last_time'] = hora_i.strftime("%H00")

                    with open('./config.json', 'w') as f:
                        json.dump(self.config_file, f)

                    f.close()
                    
                    datos_out_contactos.to_csv(contacts_path + contacts_filename + '{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H"))),index=False,encoding='UTF-16',sep=';')

                    datos_out_CF.to_csv(cf_path + cf_filename + '{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H"))),index=False,encoding='UTF-16',sep=';')

                    datos_out_tags.to_csv(tags_path + tags_filename + '{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H"))),index=False,encoding='UTF-16',sep=';')

                    datos_out_activities = self.API_Query_activities(cases_csv_name, self.CSV_Case_ids(cases_csv_name), desde, hasta)

                    activities_csv_name = activities_path + activities_filename + hora_i.strftime("%Y%m%d_%H")+'00_UTC.csv'
                    
                    datos_out_activities.to_csv(activities_csv_name, index=False, encoding='UTF-16',sep=';')

                    self.config_file['download_extract']['activities']['last_date'] = hora_i.strftime("%Y%m%d")
                    self.config_file['download_extract']['activities']['last_time'] = hora_i.strftime("%H00")

                    with open('./config.json', 'w') as f:
                        json.dump(self.config_file, f)

                    f.close()

                    datos_out_casetypes = self.API_Query_CaseTypes()

                    try:
                        if not datos_out_casetypes.empty:

                            hora_ant = hora_i - timedelta(hours=1)
                            os.remove(types_path + types_filename + '{}00_UTC.csv'.format(str(hora_ant.strftime("%Y%m%d_%H"))))

                    except Exception as E:
                        logging.warning(E)

                    datos_out_casetypes.to_csv(types_path + types_filename + '{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H"))),index=False,encoding='UTF-16',sep=';')

                    self.config_file['download_extract']['types']['last_date'] = hora_i.strftime("%Y%m%d")
                    self.config_file['download_extract']['types']['last_time'] = hora_i.strftime("%H00")

                    with open('./config.json', 'w') as f:
                        json.dump(self.config_file, f)

                    f.close()

                    datos_out_users, datos_out_users_queues = self.API_Query_Users_Queues()

                    try:
                        if not datos_out_users.empty:

                            hora_ant = hora_i - timedelta(hours=1)
                            os.remove(users_path + users_filename + '{}00_UTC.csv'.format(str(hora_ant.strftime("%Y%m%d_%H"))))

                    except Exception as E:
                        logging.warning(E)

                    datos_out_users.to_csv(users_path + users_filename + '{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H"))),index=False,encoding='UTF-16',sep=';')

                    self.config_file['download_extract']['users']['last_date'] = hora_i.strftime("%Y%m%d")
                    self.config_file['download_extract']['users']['last_time'] = hora_i.strftime("%H00")

                    with open('./config.json', 'w') as f:
                        json.dump(self.config_file, f)

                    f.close()

                    try:
                        if not datos_out_users_queues.empty:

                            hora_ant = hora_i - timedelta(hours=1)
                            os.remove(user_queues_path + user_queues_filename + '{}00_UTC.csv'.format(str(hora_ant.strftime("%Y%m%d_%H"))))

                    except Exception as E:
                        logging.warning(E)

                    datos_out_users_queues.to_csv(user_queues_path + user_queues_filename + '{}00_UTC.csv'.format(str(hora_i.strftime("%Y%m%d_%H"))),index=False,encoding='UTF-16',sep=';')

                    self.config_file['download_extract']['user_queues']['last_date'] = hora_i.strftime("%Y%m%d")
                    self.config_file['download_extract']['user_queues']['last_time'] = hora_i.strftime("%H00")

                    with open('./config.json', 'w') as f:
                        json.dump(self.config_file, f)

                    f.close()

                except Exception as E:
                    logging.warning(E)
                    pass

                i = i + 1

        else:

            logging.info("ALERTA: Rango de Query demasiado largo, ver config.json, max_days_query")


        end_time = datetime.now()
        elapsed = end_time - begin_time

        logging.info('Cantidad consultas API en esta ejecución: * ' + str(self.query_count) + ' * consultas, realizadas en ' + str(elapsed) + ' h:mm:ss:xxxxxx')

    # Execute whole script

if __name__=='__main__':
    
    API_Query_ = API_Query()
    API_Query_.PY_Query_Exec()


     






