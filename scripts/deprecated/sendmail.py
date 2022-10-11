import sys
import os
import smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from optparse import OptionParser

def main():
    usage = 'usage: %prog [options] [file ...]'
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', dest='filename', type='str', help='specify file to attach', default=None)
    parser.add_option('-e', '--email', dest='email', type='str', help='email to use for receiver, sender', default='leonhard_spiegelberg@brown.edu')
    parser.add_option('-m', '--message', dest='message', type='str', help='specify plain text message', default=None)
    parser.add_option('-b', '--body', dest='body', type='str', help='specify file for body', default=None)
    parser.add_option('-s', '--subject', dest='subject', type='str', help='subject line', default='Do not reply')
    parser.add_option('-H', '--hostname', dest='host', help='smtp server hostname', default='email-smtp.us-east-1.amazonaws.com')
    parser.add_option('-u', '--smtp-user', dest='smtp_user', help='SMTP user name', default=None)
    parser.add_option('-p', '--smtp-password', dest='smtp_password', help='SMTP password', default=None)
    (options, args) = parser.parse_args()

    if not options.message and not options.body:
        parser.error("need to specify a message via the -m option or a file to be the body via -b")

    # env does not work with option parser
    # cf. https://stackoverflow.com/questions/10551117/setting-options-from-environment-variables-when-using-argparse
    if not options.smtp_user:
        options.smtp_user = os.environ.get('SMTP_USER_NAME', None)

    if not options.smtp_password:
        options.smtp_password = os.environ.get('SMTP_PASSWORD', None)

    if not options.smtp_user or not options.smtp_password:
        parser.error("need to specify SMTP login either via options, or environment variables SMTP_USER_NAME and SMTP_PASSWORD")

    text_message = options.message if options.message else open(options.body, 'r').read()

    smtp_server = options.host
    port = 587
    sender_email = options.email
    receiver_email = options.email

    # Create a secure SSL context
    context = ssl.create_default_context()

    # HTML message
    message = MIMEMultipart("alternative")
    message["Subject"] = options.subject
    message["From"] = options.email
    message["To"] = options.email

    # email message
    message.attach(MIMEText(text_message, "plain"))

    # iterate and attach files
    for path in args:
        # Open PDF file in binary mode
        with open(path, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            "attachment; filename= {}".format(os.path.basename(path)),
        )

        message.attach(part)

    # send the actual message out
    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo() # Can be omitted
        server.starttls(context=context) # Secure the connection
        server.ehlo() # Can be omitted
        server.login(options.smtp_user, options.smtp_password)
        # TODO: Send email here
        server.sendmail(sender_email, receiver_email, message.as_string())
    except Exception as e:
        # Print any error messages to stdout
        print(e)
        sys.exit(1)
    finally:
        server.quit()

if __name__ == '__main__':
    main()
