# recruiting-codeforces
Tracks the best competitors in the codeforces platform.

This project was deployed in the AWS Cloud. Services used:

- Identity and Access Management (IAM)
- Object Storage (S3)
- Elastic Private Registry 
- Batch
- CloudWatch
- Simple Email Service (SES)

A user, a role, and a policy were created using **IAM**. The project is packed in a *distroless* Docker and the image is sent to the **Registry**. **CloudWatch** executes a **Batch job** on a weekly basis. It also provides execution logs. The codeforces API provides the updates, which are stored as a parquet file into an **S3 bucket**. Finally, the **SES** send emails with the updates to contacts.