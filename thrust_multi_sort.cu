#include <thrust/sort.h>
struct t
{
  int j,k,l;
};
int main()
{
  const int N = 6;
  int i;
  int    keys[N] = {  1,   4,   2,   8,   5,   7};
  struct t values[N]= { {3,4,5},{5,6,7},{8,9,10},{11,12,13},{14,15,16},{17,18,19}};
  //int *values2[N]= { {13,14,15},{15,16,17},{18,19,110},{111,112,113},{114,15,16},{17,18,19}};
  thrust::sort_by_key(keys, keys + N, values );
  for(i=0;i<N;i++)
  {
    printf("%d %d %d\n",values[i].j,values[i].k,values[i].l);
//    printf("%d %d %d\n",values2[i][0],values2[i][1],values2[i][2]);
  }
  printf("i=%d\n",i);
}
