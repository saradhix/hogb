#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "libpq-fe.h"
#define TESLA_PG_MAX_CONNINFO_LEN 1024
#define AGG_SUM 0
#define AGG_COUNT 1
#define AGG_MIN 2
#define AGG_MAX 3
#define AGG_AVG 4

int sort_data(int *keys, int **values, int n);
int fetch_data(char *query, int *gb_keys, int gb_keys_count, int *agg_cols,
               int *agg_funcs, int agg_funcs_count, int **keys, int ***ptrs,
               int **values, int *num_rows);
int group_by(int *keys, int *data[], int rows, int cols, int agg_funcs[],
             int agg_funcs_count, int agg_vals[], int agg_cols_count);
void emit_previous_row(int key, int agg_vals[], int agg_funcs_count);
void update_group(int key, int *row_data, int agg_cols[], int agg_vals[],
                  int agg_funcs[], int agg_funcs_count);
void reset_group_indexes(int agg_cols[], int num_agg_cols);
PGconn *connect_to_db(char *host, char *port, char *user, char *password, char *database);
#define MAX 5
#define TRUE 1
#define FALSE 0
PGresult *res = NULL;
PGconn *conn = NULL;
main()
{
  char *host=(char *)"127.0.0.1";
  char *port=(char *)"8000";
  char *user=(char *)"tesla";
  char *password=(char *)"";
  char *database=(char *)"postgres";
  int gb_col=0;
  int agg_cols[]={1,2, 3};
  int agg_funcs[]={0,0,3};//sum,max
  int num_agg_cols=sizeof(agg_cols)/sizeof(agg_cols[0]);
  int rows=-1, i,j;
  int *keys=NULL, *values=NULL, **ptrs=NULL;
  char *query=(char *)"select i, i, j, k from t4";
  time_t start,end;
  double timediff;
  conn = connect_to_db(host, port, user, password, database);
  if(!conn)
  {
    printf("Connect to backend failed\n");
    exit(1);
  }
  printf("Connection to db succeeded\n");
  //Fetch the data in the query and sort
  printf("Beginning fetching data\n");
  time(&start);
  fetch_data(query, &gb_col, 1, agg_cols, agg_funcs, num_agg_cols, &keys,
             &ptrs, &values, &rows);
  time(&end);
  timediff = difftime(end, start);
  printf("Time taken for fetch=%f\n",timediff);
#ifdef DEBUG
  for(i=0;i<rows;i++)
  {
    printf("mainptrs[%d]=%x\n",i,ptrs[i]);
  }
  //print keys
  for(i=0;i<rows;i++)
  {
    printf("Key[%d]=%d\n",i,keys[i]);
  }
  printf("printing values");
  for(i=0;i<rows;i++)
    for(j=0;j<num_agg_cols;j++)
    {
      printf("%d\n",values[i*num_agg_cols+j]);
    }
  printf("Printing thru ptrs before sorting\n");
  for(i=0;i<rows;i++)
    for(j=0;j<num_agg_cols;j++)
    {
      printf("%d\n",ptrs[i][j]);
    }
#endif
  time(&start);
  sort_data(keys, ptrs, rows);
  time(&end);
  timediff = difftime(end, start);
  printf("Time taken for sort=%lf\n",timediff);
  reset_group_indexes(agg_cols, num_agg_cols);
  time(&start);
  group_by(keys, ptrs , rows, num_agg_cols, agg_funcs, num_agg_cols, agg_cols,
           num_agg_cols);
  time(&end);
  timediff = difftime(end, start);
  printf("Time taken for aggregation=%f\n",timediff);
#ifdef DEBUG
  printf("Printing thru ptrs after sorting\n");
  for(i=0;i<rows;i++)
  {
    printf("Key=%d\n",keys[i]);
    for(j=0;j<num_agg_cols;j++)
    {
      printf("%d\n",ptrs[i][j]);
    }
  }
#endif

}

int fetch_data(char *query, int *gb_keys, int gb_keys_count, int *agg_cols,
               int *agg_funcs, int agg_funcs_count, int **pkeys, int ***pptrs,
               int **pvalues, int *num_rows)
{
  int i, j;
  char buf[1024]={0};
  int num_fields=0;
  int *keys, *values, **ptrs;
  int ival;
  PQsetSingleRowMode(conn);
  res=PQexecParams(conn, query, 0, NULL, NULL, NULL, NULL, 1);
  if(PQresultStatus(res) != PGRES_TUPLES_OK)
  {
    printf("Query failed %s\n",PQerrorMessage(conn));
    PQclear(res);
    exit(1);
  }
  num_fields = PQnfields(res);
  *num_rows = PQntuples(res);
  printf("Entered fetch_data with query=%s\n", query);
  printf("Num fields=%d, num_rows=%d\n", num_fields, *num_rows);

  //Allocate memory for the three variables
  //keys will have size num_rows*sizeof(int) because the group by keys is
  //an integer
  //ptrs will have size num_rows*sizeof(int *) because the ptr is a pointer
  //the values will have size num_rows*agg_col_count*sizeof(int) as each row
  //contains the full agg_col values

  keys = (int *)malloc(*num_rows*sizeof(int));
  if(!keys)
  {
    printf("Could not allocate memory for keys");
    return -1;
  }
  ptrs = (int **)malloc(*num_rows*sizeof(int *));
  if(!ptrs)
  {
    printf("Could not allocate memory for ptrs");
    return -1;
  }
  values = (int *)malloc(*num_rows*agg_funcs_count*sizeof(int));
  //Note that agg_funcs_count is equal to the number of aggregated columns
  if(!values)
  {
    printf("Could not allocate memory for values");
    return -1;
  };

  //Now populate the values
  for(i=0;i<*num_rows;i++)
  {
    //keys[i]=atoi(PQgetvalue(res, i, gb_keys[0]));
    keys[i]=ntohl(*((int *)PQgetvalue(res, i, gb_keys[0])));
    //printf("key=%d\n",keys[i]);
    ptrs[i]=values+i*agg_funcs_count;
    for(j=0;j<agg_funcs_count;j++)
    {
      values[i*agg_funcs_count+j]=ntohl(*((int *)PQgetvalue(res, i, agg_cols[j])));
      //printf("Value [%d]=%d\n",i*agg_funcs_count+j,values[i*agg_funcs_count+j]);
#ifdef DEBUG
      printf("Value by ptr [%d]=%d\n",j,ptrs[i][j]);
#endif
    }
    //printf("\n");
  }

  *pptrs=ptrs;
  *pkeys=keys;
  *pvalues=values;


}

PGconn *connect_to_db(char *host, char *port, char *user, char *password, char *database)
{
  char conninfo[TESLA_PG_MAX_CONNINFO_LEN]={0};
  PGconn *conn = NULL;

  snprintf(conninfo, sizeof(conninfo),"hostaddr= %s dbname = %s user = %s \
           port = %s", host, database, user,  port);

  /* Make a connection to the database*/
  conn = PQconnectdb(conninfo);

  /* Check to see that the backend connection was successfully made*/
  if(PQstatus(conn) != CONNECTION_OK)
  {
    printf("Connection to backend failed");
    return NULL;
  }
  return conn;
}
int group_by(int *keys, int *data[], int rows, int cols, int agg_funcs[], 
             int agg_funcs_count, int agg_cols[], int agg_cols_count)
{
  int i, j; //For iteration
  int key, prev_key=-1;
  int agg_vals[agg_funcs_count];
  printf("Entered group by with rows=%d cols=%d func_count=%d cols_count=%d\n",
         rows, cols, agg_funcs_count, agg_cols_count);
  memset(agg_vals, 0, agg_cols_count*sizeof(int));
  prev_key=keys[0];
  for(i=0;i<rows;i++)
  {
    key=keys[i];
    if(key != prev_key)
    {
      emit_previous_row(prev_key, agg_vals, agg_funcs_count);
      //initialise a set of counters
      memset(agg_vals, 0, agg_cols_count*sizeof(int));
    }
    update_group(key, data[i], agg_cols, agg_vals, agg_funcs, agg_funcs_count);
    prev_key=key;
  }
  emit_previous_row(prev_key, agg_vals, agg_funcs_count);
}
void emit_previous_row(int key, int agg_vals[], int agg_funcs_count)
{
  int i;
#ifndef PRINT_GROUP
  return;
#endif
  printf("Emiting group\n");
  printf("Key=%d\n", key);
  for(i=0;i<agg_funcs_count;i++)
  {
    printf("Agg[%d]=%d\t",i,agg_vals[i]);
  }
  printf("\n");
}
void update_group(int key, int *row_data, int agg_cols[], int agg_vals[],
                  int agg_funcs[], int agg_funcs_count)
{
  int i;
#ifdef DEBUG
  printf("Entered update_group with key=%d\n", key);
  printf("Current row values::\n");
  for(i=0;i<agg_funcs_count;i++)
  {
    printf("col[%d]=%d", i, row_data[i]);
  }
  printf("Current aggs before update are\n");
  for(i=0;i<agg_funcs_count;i++)
  {
    printf("agg_vals[%d]=%d\n",i,agg_vals[i]);
  }
  printf("\n");
#endif
  for(i=0;i<agg_funcs_count;i++)
  {
#ifdef DEBUG
    printf("Agg_func[%d]=%d\n",i,agg_funcs[i]);
#endif
    switch(agg_funcs[i])
    {
      case AGG_AVG:
      case AGG_SUM:agg_vals[i]+=row_data[agg_cols[i]];
                   break;
      case AGG_MAX:if(row_data[agg_cols[i]] > agg_vals[i])
                   {
                     agg_vals[i]=row_data[agg_cols[i]];
                   }
                   break;
      case AGG_MIN:if(row_data[agg_cols[i]] < agg_vals[i])
                   {
                     agg_vals[i]=row_data[agg_cols[i]];
                   }
                   break;
      case AGG_COUNT:
                   agg_vals[i]++;
                   break;

    }
  }
#ifdef DEBUG
  printf("Current aggs after update are\n");
  for(i=0;i<agg_funcs_count;i++)
  {
    printf("agg_vals[%d]=%d\n",i,agg_vals[i]);
  }
  printf("\n");
#endif
}
/* The group index columns have to be reduced by 1 because
 * the first index is the group by column which has to be selected in the 
 * query but when we aggregate, we should not aggregate the 0th column*/
void reset_group_indexes(int agg_cols[], int num_agg_cols)
{
  int i;
  for(i=0;i<num_agg_cols;i++)
  {
    agg_cols[i]--;
  }
}
