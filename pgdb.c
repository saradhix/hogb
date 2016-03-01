#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"
#define TESLA_PG_MAX_CONNINFO_LEN 1024
int sort_data(int *keys, int **values, int n);
int fetch_data(char *query, int *gb_keys, int gb_keys_count, int *agg_cols,
               int *agg_funcs, int agg_funcs_count, int **keys, int ***ptrs,
               int **values, int *num_rows);
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
  int agg_cols[]={1,2};
  int agg_funcs[]={1,2};
  int num_agg_cols=2;
  int rows=-1, i,j;
  int *keys=NULL, *values=NULL, **ptrs=NULL;
  char *query=(char *)"select i, j, k from t1 order by 1 desc";
  conn = connect_to_db(host, port, user, password, database);
  if(!conn)
  {
    printf("Connect to backend failed\n");
    exit(1);
  }
  printf("Connection to db succeeded\n");
  //Fetch the data in the query and sort
  printf("Beginning fetching data\n");
  fetch_data(query, &gb_col, 1, agg_cols, agg_funcs, num_agg_cols, &keys,
             &ptrs, &values, &rows);
/*  for(i=0;i<rows;i++)
  {
    printf("mainptrs[%d]=%x\n",i,ptrs[i]);
  }*/
  //print keys
  /*for(i=0;i<rows;i++)
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
    }*/
  sort_data(keys, ptrs, rows);
  /*printf("Printing thru ptrs after sorting\n");
  for(i=0;i<rows;i++)
  {
    printf("Key=%d\n",keys[i]);
    for(j=0;j<num_agg_cols;j++)
    {
      printf("%d\n",ptrs[i][j]);
    }
  }*/

}

int fetch_data(char *query, int *gb_keys, int gb_keys_count, int *agg_cols,
               int *agg_funcs, int agg_funcs_count, int **pkeys, int ***pptrs,
               int **pvalues, int *num_rows)
{
  int i, j;
  char buf[1024]={0};
  int num_fields=0;
  int *keys, *values, **ptrs;
  /* Start a transaction block. This is mandatory to create for any cursor*/
  res = PQexec(conn,"BEGIN");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    printf("BEGIN command failed %s\n",PQerrorMessage(conn));
    PQclear(res);
    exit(1);
  }

  snprintf(buf,sizeof(buf),"DECLARE myportal CURSOR FOR  %s",query);

  res=PQexec(conn,buf);
  if(PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    printf("DECLARE cursor failed %s\n",PQerrorMessage(conn));
    PQclear(res);
    exit(1);
  }
  PQclear(res);

  res = PQexec(conn,"FETCH ALL in myportal");
  if (PQresultStatus(res) != PGRES_TUPLES_OK)
  {
    printf("FETCH ALL failed %s\n",PQerrorMessage(conn));
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
    keys[i]=atoi(PQgetvalue(res, i, gb_keys[0]));
    ///printf("key=%d\n",keys[i]);
    ptrs[i]=values+i*agg_funcs_count;
    for(j=0;j<agg_funcs_count;j++)
    {
      values[i*agg_funcs_count+j]=atoi(PQgetvalue(res, i, agg_cols[j]));
      //printf("Value [%d]=%d\n",i*agg_funcs_count+j,values[i*agg_funcs_count+j]);
      //printf("Value by ptr [%d]=%d\n",j,ptrs[i][j]);
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
