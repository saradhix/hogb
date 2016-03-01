#include <thrust/sort.h>
int sort_data(int *keys, int **values, int N)
{
//  int i;
/*  for(i=0;i<N;i++)
  {
    printf("Before sort Key=%d addr=%x\n",keys[i],values[i]);
  }*/
  thrust::sort_by_key(keys, keys + N, values );
/*  for(i=0;i<N;i++)
  {
    printf("After sort Key=%d addr=%x\n",keys[i],values[i]);
  }
*/
  return 0;
}
