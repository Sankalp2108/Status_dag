import smtplib
import json
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
local_tz = timezone('Asia/Kolkata') 



# Open the JSON file
with open('dag_run_response.json', 'r') as file:
    # Load the JSON data
    dag_run_output = json.load(file)

# Open the JSON file
with open('task_inst_resp.json', 'r') as file:
    # Load the JSON data
    task_inst_resp = json.load(file)

# Open the JSON file
with open('output.json', 'r') as file:
    # Load the JSON data
    output = json.load(file)

def get_dag_run_info(dag_id,order_by,limit=100):
    # dag_run_resp=requests.get('http://airflow-telco-webapplications-mvp.apps.mvp.telcostackmvp.br.telefonica.com/api/v1/dags/{}/dagRuns?limit={}&order_by={}'.format(dag_id,limit,order_by),auth=('admin', 'admin'))
    # dag_run_output=dag_run_resp.json()
    # print("Dag run Response : " , dag_run_output)
    if dag_run_output['total_entries']>0:
        return dag_run_output
    else:
        return False

def sort_dag_running(dag_run_resp,dag_id):
    sorted_dag_runs=[]
    start_date=0
    start_date_obj=0
    total_running_tasks=0
    for dag_run in dag_run_resp:
        if dag_run['state']=='failed' or dag_run['state']=='queued':
            pass
        elif dag_run['state']=='running':
            date_str_without_colon = dag_run['start_date'][:-3] + dag_run['start_date'][-2:]
            parsed_date = datetime.strptime(date_str_without_colon, '%Y-%m-%dT%H:%M:%S.%f%z')
            temp_date_obj=parsed_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
            if start_date==0 or start_date_obj>temp_date_obj:
                start_date=dag_run['start_date']
                start_date_obj=temp_date_obj
            # task_inst=requests.get('http://airflow-telco-webapplications-mvp.apps.mvp.telcostackmvp.br.telefonica.com/api/v1/dags/{}/dagRuns/{}/taskInstances'.format(dag_id,dag_run['dag_run_id']),auth=('admin', 'admin'))
            # task_inst_resp=task_inst.json()
            # print("task_inst_resp : ",task_inst_resp)
            for instance in task_inst_resp["task_instances"]:
                if instance['state']=='running':
                    total_running_tasks+=1
                
#             sorted_dag_runs.append(dag_run)
        else:
            return start_date,total_running_tasks
    return start_date,total_running_tasks
        

def check_running_dag(dag_id):
    total_task_instances=0
    all_dag_runs=get_dag_run_info(dag_id,'state')
#     print(all_dag_runs)
    if all_dag_runs:
        task_instances=sort_dag_running(all_dag_runs['dag_runs'],dag_id)
        print("output",task_instances)
        if task_instances[0]!=0 and task_instances[1]>5:
            dag_info={'DAG Name':dag_id,'Total Running Instances':task_instances[1],'Stuck Since':task_instances[0],'Latest DAG Status':'running'}
            return dag_info
        else:
            return 'no_running'

    else:
            return 'no_running'    


def sort_dag_failed(dag_run_resp):
    sorted_dag_run=None
    start_date=0
    start_date_obj=0
    for dag_run in dag_run_resp:
#         print(dag_run)
        if dag_run['state']=='failed':
            date_str_without_colon = dag_run['start_date'][:-3] + dag_run['start_date'][-2:]
            parsed_date = datetime.strptime(date_str_without_colon, '%Y-%m-%dT%H:%M:%S.%f%z')
            temp_date_obj=parsed_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
            if start_date==0 or start_date_obj<temp_date_obj:
                start_date=dag_run['start_date']
                start_date_obj=temp_date_obj
                sorted_dag_run=dag_run
        else:
            return sorted_dag_run
        

def check_failling_dag(dag_id):
    all_dag_runs=get_dag_run_info(dag_id,'state')
    if all_dag_runs:
        failed_dag_run=sort_dag_failed(all_dag_runs['dag_runs'])
        if failed_dag_run!=None:
            failed_dag_info={'DAG Name':dag_id,'Failed Since':failed_dag_run['end_date'],'Latest DAG Status':'failed'} 
            return failed_dag_info 
        else:
            return 'no_failed'      

    
    else:
        return 'no_failed'      
    

def email_body_table(dag_info,email_body):
#         email_body="<table><thead><tr>"
        for key in dag_info[0].keys():
            email_body=email_body+'<th>'+key+'</th>'
        email_body=email_body+'</tr></thead><tbody>'
        for i in dag_info:
            email_body=email_body+'<tr>'
            for j in i:
                email_body=email_body+'<td align=center>'+str(i[j])+'</td>'
            email_body=email_body+'</tr>'
        email_body=email_body+'</tbody></table>' 
        return email_body



# def send_mail(subject_email,email_body):

#     # Provide the SMTP server details
#     smtp_server = "smtp.gmail.com"
#     smtp_port = 25

#     # Provide the sender and multiple recipient email addresses
#     sender_email = "mayur.deshpande@iauro.com"
#     recipient_email = "mayur.deshpande@iauro.com"
    

#     # Provide the email subject and message
#     subject = subject_email
#     message = email_body   

#     try:
#         # Create the email message
#         msg = MIMEMultipart()
#         msg["From"] = sender_email
#         msg["To"] = recipient_email
#         msg["Subject"] = subject

#         # Attach the message body
#         body = MIMEText(message, "html")
#         msg.attach(body)

#         # Connect to the SMTP server
#         server = smtplib.SMTP(smtp_server, smtp_port)

#         # Send the email
#         server.sendmail(sender_email, recipient_email , msg.as_string())

#         # Close the connection to the SMTP server
#         server.quit()

#         return "Email sent successfully."

#     except smtplib.SMTPException as e:
#         return "Failed to send email. Error: {}".format(str(e))


def send_mail(subject,email_body):
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('xrrishdummy@gmail.com', 'fapizjbrcbhnkhfi')
    
    message = MIMEMultipart()
    
    body = MIMEText(email_body, 'html')
    message.attach(body)

    
#     message = EmailMessage()
    message['From'] = 'xrrishdummy@gmail.com'
    # message['To'] = 'mayur.deshpande@iauro.com'
    message['To'] = 'sankalpmohate@gmail.com'
    message['Subject'] = subject

#     smtp_server.send_message(message, 'xrrishdummy@gmail.com', ' mayur.deshpande@iauro.com')
    smtp_server.sendmail(message['From'], message['To'], message.as_string())

    smtp_server.quit()
        


def all_dag_fail_run_status():
    all_running_dag=[]
    all_failed_dag=[]
    # response_dags=requests.get('http://airflow-telco-webapplications-mvp.apps.mvp.telcostackmvp.br.telefonica.com/api/v1/dags?limit=100&only_active=true',auth=('admin', 'admin'))
    # print(response_dags.status_code)
    # output=response_dags.json()
    # print("output : " , output)
    all_dags=output['dags']
    print("All dags : " , all_dags)
    # send_running_mail=False
    # send_failed_mail=False
    for dag in all_dags:
        dag_run_info=check_running_dag(dag['dag_id'])
        if dag_run_info=='no_running':
            pass
        else:
            all_running_dag.append(dag_run_info)
        dag_failed_info=check_failling_dag(dag['dag_id'])
        if dag_failed_info=='no_failed':
            pass
        else:
            all_failed_dag.append(dag_failed_info)
    print("All Running dag : " , all_running_dag)
    print("All Failed dag : " , all_failed_dag)
    if len(all_running_dag)>0:
        running_body=email_body_table(dag_info=all_running_dag,email_body="<table border=2 ><thead><h4>DAG's with Multiple Running Instances</h4><tr>")
        if len(all_failed_dag)>0:
            email_body=email_body_table(dag_info=all_failed_dag,email_body=running_body+"</br></br></br></br></br></br><table border=2 ><thead><h4>DAG's in Error State</h4><tr>")
            print(email_body)
            send_mail("DAG's STATUS DEV ENV",email_body=email_body)
        else:
            running_body=running_body+"</br></br></br></br></br></br><h4>Failed Dag's are Not Found!</h4>" 
            send_mail("DAG's STATUS DEV ENV",email_body=running_body)
            
    elif len(all_failed_dag)>0:   
        failed_body=email_body_table(dag_info=all_failed_dag,email_body="""
<table border="2">
        <thead style="text-align: center;">
            <tr>
                <th colspan="3"><h4>DAG's in Running State</h4></th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Dag Name</td>
                <td>Running Since</td>
                <td>Status</td>
            </tr>
        </tbody>
    </table>  
        """+"""</br></br></br></br><table border=2 ><thead style="text-align: center;"><h4>DAG's in Error State</h4><tr>""")
        send_mail("DAG's STATUS DEV ENV",email_body=failed_body)
                        
    else:
        print('no Running and Failed dags are founded')     


    
default_args = {
    'owner': 'airflow',
    'start_date': local_tz.localize(datetime(2023,5,30)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False,

}

with DAG(
    dag_id='dags-status-check-dag',
    default_args=default_args,
    description="Checking all DAG's status",
    schedule_interval=timedelta(minutes=30),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=all_dag_fail_run_status,
        
    )
task1 

