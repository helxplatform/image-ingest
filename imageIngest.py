#!/usr/bin/env python3
import sys
import yaml
import datetime
import os
from googleapiclient import discovery
import google.auth
#############################################################################################
# This code is responsible for loading DICOM data from a given study into a set of Google 
# Health datasets, one for each consent group in the study.  Each patient in the study is 
# generally represented by more than one DICOM file, but no patient is in more than one
# consent group. Intially, the code requires that the DICOM files have already been loaded
# into a GCP bucket, but this restriction may eventually be relaxed.
# This code has the following specific responsibilities:
#    1) Read the specified yaml file for needed configs including account info
#    2) Create a Google DICOM dataset for each separate consent group in the study
#    3) Import each file from the bucket to the Google Health API
#    4) Add the appropriate permissions for the dataset
#    5) Optionally export the datasets to BigQuery
#############################################################################################


# Code credit: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/healthcare/api-client/v1/datasets/datasets.py

# [START healthcare_get_client]
def get_client():
    """Returns an authorized API client by discovering the Healthcare API and
    creating a service object using the service account credentials in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    api_version = 'v1'
    service_name = 'healthcare'

    try:
       return discovery.build(service_name, api_version)
    except Exception as theException:
       print (theException)
# [END healthcare_get_client]

# [START healthcare_create_dataset]
def create_dataset(project_id, cloud_region, dataset_id):
    """Creates a dataset."""
    client = get_client()
    dataset_parent = 'projects/{}/locations/{}'.format(
        project_id, cloud_region)

#   try:
    request = client.projects().locations().datasets().create(
        parent=dataset_parent, body={}, datasetId=dataset_id)

    response = request.execute()
#   except Exception as theException:
#      print(theException)
    print('Created dataset: {}'.format(dataset_id))
    return response
# [END healthcare_create_dataset]

# [START healthcare_dataset_set_iam_policy]
def set_dataset_iam_policy(
        project_id,
        cloud_region,
        dataset_id,
        member,
        role,
        etag=None):
    """Sets the IAM policy for the specified dataset.
        A single member will be assigned a single role. A member can be any of:
        - allUsers, that is, anyone
        - allAuthenticatedUsers, anyone authenticated with a Google account
        - user:email, as in 'user:somebody@example.com'
        - group:email, as in 'group:admins@example.com'
        - domain:domainname, as in 'domain:example.com'
        - serviceAccount:email,
            as in 'serviceAccount:my-other-app@appspot.gserviceaccount.com'
        A role can be any IAM role, such as 'roles/viewer', 'roles/owner',
        or 'roles/editor'
    """
    client = get_client()
    dataset_name = 'projects/{}/locations/{}/datasets/{}'.format(
        project_id, cloud_region, dataset_id)

    policy = {
        "bindings": [
            {
                "role": role,
                "members": [
                    member
                ]
            }
        ]
    }

    if etag is not None:
        policy['etag'] = etag

    request = client.projects().locations().datasets().setIamPolicy(
        resource=dataset_name, body={'policy': policy})
    response = request.execute()

    print('etag: {}'.format(response.get('name')))
    print('bindings: {}'.format(response.get('bindings')))
    return response
# [END healthcare_dataset_set_iam_policy]

# Code credit:  https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/healthcare/api-client/v1/datasets/datasets.py
# [START healthcare_create_dicom_store]
def create_dicom_store(project_id, cloud_region, dataset_id, dicom_store_id):
    """Creates a new DICOM store within the parent dataset."""
    client = get_client()
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )

    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .create(parent=dicom_store_parent, body={}, dicomStoreId=dicom_store_id)
    )

    response = request.execute()
    print("Created DICOM store: {}".format(dicom_store_id))
    return response
# [END healthcare_create_dicom_store]

# Code credit:  https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/healthcare/api-client/v1/dicom/dicom_stores.py
# [START healthcare_import_dicom_instance]
def import_dicom_instance(
    project_id, cloud_region, dataset_id, dicom_store_id, content_uri
):
    """Import data into the DICOM store by copying it from the specified
    source.
    """
    client = get_client()
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )
    dicom_store_name = "{}/dicomStores/{}".format(dicom_store_parent, dicom_store_id)

    body = {"gcsSource": {"uri": "gs://{}".format(content_uri)}}

    # Escape "import()" method keyword because "import"
    # is a reserved keyword in Python
    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .import_(name=dicom_store_name, body=body)
    )

    response = request.execute()
    print("Imported DICOM instance: {}".format(content_uri))

    return response


# [END healthcare_import_dicom_instance]

# [START healthcare_dicom_store_set_iam_policy]
def set_dicom_store_iam_policy(
    project_id, cloud_region, dataset_id, dicom_store_id, member, role, etag=None
):
    """Sets the IAM policy for the specified dicom store.
        A single member will be assigned a single role. A member can be any of:
        - allUsers, that is, anyone
        - allAuthenticatedUsers, anyone authenticated with a Google account
        - user:email, as in 'user:somebody@example.com'
        - group:email, as in 'group:admins@example.com'
        - domain:domainname, as in 'domain:example.com'
        - serviceAccount:email,
            as in 'serviceAccount:my-other-app@appspot.gserviceaccount.com'
        A role can be any IAM role, such as 'roles/viewer', 'roles/owner',
        or 'roles/editor'
    """
    client = get_client()
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )
    dicom_store_name = "{}/dicomStores/{}".format(dicom_store_parent, dicom_store_id)

    policy = {"bindings": [{"role": role, "members": [member]}]}

    if etag is not None:
        policy["etag"] = etag

    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .setIamPolicy(resource=dicom_store_name, body={"policy": policy})
    )
    response = request.execute()

    print("etag: {}".format(response.get("name")))
    print("bindings: {}".format(response.get("bindings")))
    return response


# [END healthcare_dicom_store_set_iam_policy]

def main():

   if len(sys.argv) < 2:
      print ("usage: imageIngest.py yamlFile")
      return

   thisExecutable = sys.argv[0]
   yamlFile = sys.argv[1]

   # Open the input yamlFile
   inFile = open(yamlFile)

   # 1) Read the yaml file and grab the scripts and shortcuts
   parsedYaml = yaml.load(inFile, Loader=yaml.FullLoader)
   print (parsedYaml)
   print (parsedYaml["study"])
   study  =  (parsedYaml["study"])
   print (study["id"])
   project = parsedYaml["project"]
   region =  parsedYaml["region"]
   studyId = study["id"]
   print ("project " + project + " studyId " + studyId)

   consentGroups = study["consent-groups"]
   print (consentGroups)

   # 2) The consent groups are currently in the yaml. These may eventually come from the 
   #    telemetry file
   for i in range(len(consentGroups)):
      thisConsentGroup = consentGroups[i]
      for key in thisConsentGroup:
         print (key)

         # Create the requested dataset.
         thisDataSetName = "dataset--" + studyId + "--" + key
         create_dataset(project, region, thisDataSetName)

         thisDataStoreName = studyId + "--" + key
         print (thisDataSetName)

         # Create the dicom store
         create_dicom_store(project, region, thisDataSetName, thisDataStoreName)

         # 3) Import the data.  Note that we are assuming that the datastore name and the 
         # bucket name are the same
         uri = thisDataStoreName + "/**.dcm"
         import_dicom_instance(project, region, thisDataSetName, thisDataStoreName, uri)
         print (type(thisConsentGroup[key]))
         print (thisConsentGroup[key])
         # The values of the key are the address we want to allow to view the data.
         userList = []
         allowedEmails = thisConsentGroup.get(key)
         for thisEmail in allowedEmails:
            thisUserEmail = "user:" + thisEmail
            print ("Giving permission to " + thisEmail + " as " + thisUserEmail)
            userList.append(thisUserEmail)

         # Note that the documentation for this Google function says it sets a single
         # member to a single role. In fact if you pass a list, it adds the role
         # for each member of the list.
         # It may be that we are going to want to use groups instead of individual accounts,
         # but it works for the moment.
         set_dataset_iam_policy( project, 
                                     region, 
                                     thisDataSetName, 
                                     userList, "roles/viewer", etag=None)

if __name__ == "__main__":
   main()
