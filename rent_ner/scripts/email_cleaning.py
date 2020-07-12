import os
import eml_parser
import jsonlines
import logging
from tqdm import tqdm
from multiprocessing import Pool

logger = logging.getLogger(__name__)

def extract_body_and_subject_single(email_file):
    ep = eml_parser.EmlParser(include_raw_body=True)
    email = ep.decode_email(email_file)
    return {'subject': extract_if_possible(email_file, email, 'header','subject'),
           'body': extract_if_possible(email_file, email, 'body', 0, 'content'),
           'body_html': extract_if_possible(email_file, email, 'body', 1, 'content')}


def extract_body_and_subject(emails_dir):
    pool = Pool(2)
    emails = [emails_dir + f for f in os.listdir(emails_dir)]
    return list(tqdm(pool.imap_unordered(extract_body_and_subject_single, emails), total=len(emails)))


def extract_if_possible(file_name, nested_obj, *email_keys):
    value = nested_obj
    for key in email_keys:
        try:
            value = value[key]
        except Exception as err:
            logger.info(file_name + " failed, due to: " + str(err))
            return None
    return value

def main():
    parsed_emails = []
    for date in ['for-rent-20200103/', 'for-rent-20200704/']:
        parsed_emails.extend(extract_body_and_subject("/Users/jimmy/Documents/data/for-rent-email-dump/" + date))
    
    with jsonlines.open("/Users/jimmy/Documents/data/for-rent-email-dump/parsed_emails.json", "w") as writer:
        writer.write_all(parsed_emails)



if __name__ == "__main__":
    main()

