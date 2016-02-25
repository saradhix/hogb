#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define AGG_SUM 0
#define AGG_COUNT 1
#define AGG_MIN 2
#define AGG_MAX 3
#define AGG_AVG 4
int print_array(int *x[], int rows, int cols);
void group_by(int *data[], int rows, int cols, int gb_col, int agg_funcs[], 
              int agg_funcs_count, int agg_cols[], int agg_cols_count);
void emit_previous_row(int key, int agg_vals[], int agg_funcs_count);
void update_group(int key, int *row_data, int agg_cols[], int agg_vals[],
                  int agg_funcs[], int agg_funcs_count);
int main()
{
  int **x;
  int i,j, rows=10, cols=2;
  int gb_column = 0;
  int agg_funcs[]={AGG_SUM,AGG_MAX, AGG_COUNT};
  int agg_cols[]={1,1};//aggregating sum(j), max(j)
  x=malloc(sizeof(int *)*10);
  for(i=0;i<10;i++)
    x[i]=malloc(sizeof(int)*20);

  x[0][0]=1;
  x[0][1]=10;
  x[1][0]=1;
  x[1][1]=100;
  x[2][0]=2;
  x[2][1]=20;
  x[3][0]=2;
  x[3][1]=200;
  x[4][0]=3;
  x[4][1]=30;
  x[5][0]=3;
  x[5][1]=300;
  x[6][0]=4;
  x[6][1]=40;
  x[7][0]=4;
  x[7][1]=400;
  x[8][0]=5;
  x[8][1]=50;
  x[9][0]=5;
  x[9][1]=500;

  print_array(x,10,2);
  //Trying to group by query
  //select i, sum(j), count(j) from t1;
  group_by(x, rows, cols, gb_column, agg_funcs, 3, agg_cols, 3);
}

int print_array(int *x[], int rows, int cols)
{
  int i, j;
  for(i=0;i<rows;i++)
  {
    for(j=0;j<cols;j++)
    {
      printf("%d ", x[i][j]);
    }
    printf("\n");
  }
}

void group_by(int *data[], int rows, int cols, int gb_col, int agg_funcs[], 
              int agg_funcs_count, int agg_cols[], int agg_cols_count)
{
  int i, j; //For iteration
  int key, prev_key=-1;
  int agg_vals[agg_funcs_count];
  printf("Entered group by with rows=%d cols=%d gb_col=%d func_count=%d cols_count=%d\n",
         rows, cols, gb_col, agg_funcs_count, agg_cols_count);
  memset(agg_vals, 0, agg_cols_count*sizeof(int));
  prev_key=data[0][gb_col];
  for(i=0;i<rows;i++)
  {
    key=data[i][gb_col];
    if(key != prev_key)
    {
      emit_previous_row(prev_key, agg_vals, agg_funcs_count);
      //initialise a set of counters
      memset(agg_vals, 0, agg_cols_count*sizeof(int));
    }
    update_group(key, data[i], agg_cols, agg_vals, agg_funcs, agg_funcs_count);
    prev_key=key;
  }
}

void emit_previous_row(int key, int agg_vals[], int agg_funcs_count)
{
  int i;
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
  //printf("Entered update_group with key=%d\n", key);
  ///printf("CUrrent row values::\n");
  for(i=0;i<agg_funcs_count;i++)
  {
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
}
