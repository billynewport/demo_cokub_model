# Demo1 docker prompt

 Create a DataSurface Yellow system on my remote Kubernetes.
 Configuration:
    - Model repository: billynewport/demo_cokub_model
    - Airflow DAG repository: billynewport/demo_cokub_airflow
    - GitHub username: billynewport
    - Namespace: demo1-cokub
    - DataSurface version: 1.1.0
Environment variables already set:
    - GITLAB_CUSTOMER_USER / GITLAB_CUSTOMER_TOKEN - GitLab registry credentials
    - GH_DEMO2_AIRFLOW_USER / GH_DEMO2_AIRFLOW_PAT - credentials for Airflow DAG sync
    GitHub PAT for model repository access: GITHUB_MODEL_PULL_TOKEN
I'm logged into GitHub as billynewport so normal git commands work.
I want to use this projects setup-walkthrough skill
