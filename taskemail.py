from tabulate import tabulate
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def generate_daily_report():
    daily_cron= [
    ["Filename", "Stage", "Status", "Date", "Company Name"],
    ["ITALY.txt", "GETFILE", "done", "5-2-24", "ncl"],
    ["ITALY_Itinerary.txt", "CONNECT mysql", "connected", "5-2-24", ""],
    ["", "loaddataNCL", "loaded", "6-2-24", ""],
    ["", "loaddataNCLit", "loaded", "6-2-24", ""],
    ["", "filedownloadintxt", "two files download", "6-2-24", ""],
    ["", "filedownload in zip", "two files download", "7-2-24", ""]
]
        
   
    return tabulate(daily_cron, headers='keys', tablefmt="html",maxcolwidths=[None, 8])

def send_email(html_table):
    sender_email = "ghevarikar.shreyash@gmail.com"
    receiver_email = "ghevarikar.shreyash@gmail.com"  
    password = "dkvc alji chtb zrdr" 
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = "Daily Cron Report"
    
    html_content = f"""
    <html>
    <head>
      <style>
        table {{
          border-collapse: collapse;
          width: 100%;
        }}
        th, td {{
          border: 1px solid #dddddd;
          text-align: left;
          padding: 8px;
        }}
        th {{
          background-color:white;
        }}
      </style>
    </head>
    <body>
    {html_table}
    </body>
    </html>
    """

    msg.attach(MIMEText(html_content, 'html'))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email sent successfully")
    except Exception as e:
        print(f"An error occurred while sending the email: {e}")

if __name__ == "__main__":
    table = generate_daily_report()
    send_email(table)

