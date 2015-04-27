# Small script to notify myself
# when a job ends. 

# Example usage: python notify.py morganeciot@gmail.com "scraping reddit comments" enterprise

import sys
import smtplib

email = sys.argv[1]
job = sys.argv[2]
host = sys.argv[3]

lab_email = "redresearchndmcgill@gmail.com"
lab_password = "reddit_topics31050"

body = "Job " + job + " terminated on server " + host
message = "\r\n".join([
  "From:" + lab_email,
  "To:" + email,
  "Subject: Job termination notification",
  "",
  body 
  ])
server = smtplib.SMTP("smtp.gmail.com", 587)
server.ehlo()
server.starttls()
server.login(lab_email, lab_password)
server.sendmail(lab_email, [email], message)
server.close()
