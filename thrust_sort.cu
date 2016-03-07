#include <thrust/device_vector.h>
#include <thrust/host_vector.h>
#include <thrust/sort.h>
#include <time.h>
int sort_data(int *keys, int **values, int N)
{
//  int i;
/*  for(i=0;i<N;i++)
  {
    printf("Before sort Key=%d addr=%x\n",keys[i],values[i]);
  }*/
  time_t start, end;
  thrust::host_vector<int> h_keys(N);
  thrust::host_vector<int *> h_vals(N);
  time(&start);
  for(int i=0;i<N;i++)
  {
    h_keys[i]=keys[i];
    h_vals[i]=values[i];
  }
  time(&end);
  printf("Time taken for h->d is %f\n",difftime(end, start));
  thrust::device_vector<int> d_keys = h_keys;
  thrust::device_vector<int *> d_vals = h_vals;
  thrust::sort_by_key(d_keys.begin(), d_keys.end(), d_vals.begin() );
  thrust::copy(d_keys.begin(), d_keys.end(), h_keys.begin());
  thrust::copy(d_vals.begin(), d_vals.end(), h_vals.begin());
  time(&start);
  for(int i=0;i<N;i++)
  {
    keys[i]=h_keys[i];
    values[i]=h_vals[i];
  }
  time(&end);
  printf("Time taken for d->h is %f\n",difftime(end, start));
  /*  for(i=0;i<N;i++)
      {
      printf("After sort Key=%d addr=%x\n",keys[i],values[i]);
      }
   */
  return 0;
}
