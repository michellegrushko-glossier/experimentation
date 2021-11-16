library(data.table)
library(plyr)
library(tidyverse)
library(DBI)
library(janitor)
options(scipen = 999)


con <- DBI::dbConnect(odbc::odbc(),
                      Driver       = "Snowflake",
                      Server       = "np83438.us-east-1.snowflakecomputing.com",
                      UID          = "MICHELLEG",
                      PWD          = "redacted",
                      Database     = "PLAYGROUND",
                      Warehouse    = "ADHOC",
                      Schema       = "PUBLIC")


personalization <- DBI::dbGetQuery(con, "WITH order_metrics AS (
  SELECT 
  order_email_address, 
  SUM(ORDER_NET_REVENUE_USD_AMOUNT) AS study_total_rev, 
  AVG(ORDER_NET_REVENUE_USD_AMOUNT) AS study_avg_rev,
  COUNT(DISTINCT ORDER_NUMBER) AS study_count_orders,
  SUM(UNITS_PER_TRANSACTION) as study_total_units,
  AVG(UNITS_PER_TRANSACTION) as study_average_units
  FROM dbt.production.dim_orders
  WHERE 
  order_state='complete'
    AND UPPER(dim_orders.channel) IN ('ECOMM', 'RETAIL')
    AND is_test_order='false'
    AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', CAST(dim_orders.completed_at AS TIMESTAMP_NTZ))) BETWEEN '2021-10-15' AND '2021-11-15'
  GROUP BY order_email_address), 
email_metrics AS (
  SELECT DISTINCT 
  fact_braze_message_events.RECIPIENT_ADDRESS,
  fact_braze_message_events.VARIANT_NAME,
  CASE WHEN UNSUBSCRIBE_COUNT >= 1 
       THEN 1 ELSE 0 END AS unsubscribed,
  CASE WHEN OPEN_COUNT >= 1 
       THEN 1 ELSE 0 END as opened_email,
  CASE WHEN DELIVERY_COUNT >= 1
       THEN 1 ELSE 0 END as delivered_email
  FROM DBT.PRODUCTION.FACT_BRAZE_MESSAGE_EVENTS AS fact_braze_message_events
  WHERE 
    fact_braze_message_events.CAMPAIGN_NAME = '20211015_one-off_super-bounce-personalization-experiment'
    AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', CAST(fact_braze_message_events.delivered_at AS TIMESTAMP_NTZ))) >= '2021-10-15')
SELECT 
  COALESCE(email_metrics.RECIPIENT_ADDRESS, order_metrics.order_email_address) AS email_address,
  email_metrics.VARIANT_NAME,
  COALESCE(study_total_rev,0) AS study_total_rev, 
  COALESCE(study_avg_rev, 0) AS study_avg_rev,
  COALESCE(study_count_orders, 0) AS study_count_orders, 
  COALESCE(study_total_units, 0) AS study_total_units,
  COALESCE(study_average_units, 0) AS study_average_units,
  CASE WHEN study_count_orders >=1 
       THEN 1 ELSE 0 END AS converted, 
  COALESCE(unsubscribed,0) AS unsubscribed, 
  COALESCE(opened_email,0) AS opened_email,
  COALESCE(delivered_email,0) AS delivered_email
  FROM order_metrics
RIGHT JOIN email_metrics
ON email_metrics.RECIPIENT_ADDRESS = order_metrics.order_email_address")

#pull in total customers per variant
tabyl(personalization$VARIANT_NAME)
#personalization$VARIANT_NAME      n   percent
#dynamic-recommendations 389455 0.4995594
#static-recommendations 390142 0.5004406

#calculate averages among metrics
experiment_summary <- personalization %>% 
  group_by(VARIANT_NAME) %>%
  summarise_at(c("STUDY_TOTAL_REV",
                 "STUDY_AVG_REV",
                 "STUDY_COUNT_ORDERS",
                 "STUDY_TOTAL_UNITS",
                 "STUDY_AVERAGE_UNITS"), mean, na.rm = TRUE)


t.test(STUDY_TOTAL_REV ~ VARIANT_NAME, data = personalization)
#there is a stat sig difference but i believe it is being driven by an outlier of a customer with 25 orders in the static recommendations bucket (order total 7326.00000)
#data:  STUDY_TOTAL_REV by VARIANT_NAME
#t = -1.2109, df = 776106, p-value = 0.2259
#alternative hypothesis: true difference in means is not equal to 0
#95 percent confidence interval:
#  -0.16713486  0.03947984
#sample estimates:
#  mean in group dynamic-recommendations  mean in group static-recommendations 
#4.137916                              4.201743 

t.test(STUDY_AVG_REV ~ VARIANT_NAME, data = personalization)
#no difference in average order revenue
#data:  STUDY_AVG_REV by VARIANT_NAME
#t = -0.83808, df = 779234, p-value = 0.402
#alternative hypothesis: true difference in means is not equal to 0
#95 percent confidence interval:
#  -0.10878812  0.04361895
#sample estimates:
#  mean in group dynamic-recommendations  mean in group static-recommendations 
#3.700076                              3.732661 

t.test(STUDY_COUNT_ORDERS ~ VARIANT_NAME, data = personalization)
#there is a stat sig difference but i believe it is being driven by an outlier of a customer with 25 orders in the static recommendations bucket (order total 7326.00000)
#data:  STUDY_COUNT_ORDERS by VARIANT_NAME
#t = -2.2329, df = 779132, p-value = 0.02556
#alternative hypothesis: true difference in means is not equal to 0
#95 percent confidence interval:
#  -0.0027854864 -0.0001813047
#sample estimates:
#  mean in group dynamic-recommendations  mean in group static-recommendations 
#0.07171047                            0.07319386 

t.test(STUDY_TOTAL_UNITS ~ VARIANT_NAME, data = personalization)
#no difference in total units
#data:  STUDY_TOTAL_UNITS by VARIANT_NAME
#t = -1.3817, df = 772849, p-value = 0.1671
#alternative hypothesis: true difference in means is not equal to 0
#95 percent confidence interval:
#  -0.009837567  0.001702397
#sample estimates:
#  mean in group dynamic-recommendations  mean in group static-recommendations 
#0.2082449                             0.2123124 

t.test(STUDY_AVERAGE_UNITS ~ VARIANT_NAME, data = personalization)
#no difference in average number of units purchased
#data:  STUDY_AVERAGE_UNITS by VARIANT_NAME
#t = -1.1922, df = 778964, p-value = 0.2332
#alternative hypothesis: true difference in means is not equal to 0
#95 percent confidence interval:
#  -0.006346734  0.001545868
#sample estimates:
#  mean in group dynamic-recommendations  mean in group static-recommendations 
#0.1882935                             0.1906939 

#calculate rates
tabyl(personalization, CONVERTED, VARIANT_NAME, show_na = TRUE)
#CONVERTED dynamic-recommendations static-recommendations
#0                  364228                 364470
#1                   25227                  25672

prop.test( x = c(25227, 25672), n = c(389455, 390142), conf.level = 0.95 )
#no difference
#data:  c(25227, 25672) out of c(389455, 390142)
#X-squared = 3.3487, df = 1, p-value = 0.06726
#alternative hypothesis: two.sided
#95 percent confidence interval:
#  -0.00212583829  0.00007274232
#sample estimates:
#  prop 1     prop 2 
#0.06477513 0.06580168 



tabyl(personalization, UNSUBSCRIBED, VARIANT_NAME, show_na = TRUE)
#UNSUBSCRIBED dynamic-recommendations static-recommendations
#0                  388970                 389659
#1                     485                    483


prop.test( x = c(485, 483), n = c(389455, 390142), conf.level = 0.95 )
#no difference in unsubscribe rates
#data:  c(485, 483) out of c(389455, 390142)
#X-squared = 0.0035516, df = 1, p-value = 0.9525
#alternative hypothesis: two.sided
#95 percent confidence interval:
#  -0.0001515887  0.0001662272
#sample estimates:
#  prop 1      prop 2 
#0.001245330 0.001238011 

tabyl(personalization, OPENED_EMAIL, VARIANT_NAME, show_na = TRUE)
#RECIEVED_EMAIL dynamic-recommendations static-recommendations
#0                  281561                 282336
#1                  107894                 107806

prop.test( x = c(107894, 107806), n = c(389455, 390142), conf.level = 0.95 )
#no difference in recieved rates
#data:  c(107894, 107806) out of c(389455, 390142)
#X-squared = 0.49208, df = 1, p-value = 0.483
#alternative hypothesis: two.sided
#95 percent confidence interval:
#  -0.001275257  0.002702047
#sample estimates:
#  prop 1    prop 2 
#0.2770384 0.2763250 

tabyl(personalization, DELIVERED_EMAIL, VARIANT_NAME, show_na = TRUE)
#everyone had the email delivered
