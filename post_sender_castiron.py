import csv
import os
import re

import requests


def content_collector(path):
    """
    Given a path to the folder with files,
    iterate through each file and get content
    return: dict with name of files and content
    """
    content_dict = {}
    for root, dirs, files in os.walk(path):
        for filename in files:
            if filename.endswith('.csv'):
                file_path = os.path.join(root, filename)
                with open(file_path) as file:
                    customer_data = []
                    reader = csv.reader(file)
                    next(reader)
                    for row in reader:
                        customer_data.append(row[2])
                    content_dict[file_path.split('/')[-1]] = customer_data
    return content_dict


def post_data(customer_id, content, job_link):
    """
    Send post request with given content data to the JOB_LINK
    print: customer id, code status and response from server
    """
    r = requests.post(job_link, data=content)
    print("CustomerId:{0}\nStatus:{1}\nResponse:{2}\n-----------\n".format(customer_id, r.status_code, r.reason))


if __name__ == "__main__":

    JOB_LINK = "http://esbaraciap02d.burberry.corp:8500/jms-prod/write_new_mq?queue=BUR.CIRRUS.C4C.INBOUND"
    PATH = os.getcwd() + "/attachments/"

    for file_name, content in content_collector(PATH).items():
        for data in content:
            try:
                customerId = re.findall(r"(?<=<CustomerIdentifier>)(.*)(?=</CustomerIdentifier>)", data,)[0]
                matches = re.findall(r"(?=<ModifiedDate>)(.*)(?=</ModifiedDate>)", data)[0]
                trunc_line = re.findall(r"(?=\.)(.*)", matches)[0]
                new_matches = matches.replace(trunc_line, "Z")
                new_content = data.replace(matches, new_matches)
                post_data(customer_id=customerId, content=new_content, job_link=JOB_LINK)
            except IndexError:
                pass
            continue
