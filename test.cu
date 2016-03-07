#include <thrust/device_vector.h>
#include <thrust/host_vector.h>
#include <thrust/sort.h>
#include <time.h>
thrust::host_vector<int> get_data();
int get_data_by_args(thrust::host_vector<int> &v, int &n);
main()
{
  int n=0;
  thrust::host_vector<int> t = get_data();
  thrust::host_vector<int> v;
  printf("Printing t\n");
  for(int i=0;i<10;i++)
  {
    printf("%d\n",t[i]);
  }
  get_data_by_args(v,n);
  printf("Printing v\n");
  for(int i=0;i<n;i++)
  {
    printf("%d\n",v[i]);
  }
}
thrust::host_vector<int> get_data()
{
  thrust::host_vector<int> v(10);
  for(int i=0;i<10;i++)
    v[i]=i;
  return v;
}
int get_data_by_args(thrust::host_vector<int> &v, int &n)
{
  v.resize(10);//else core dump
  for(int i=0;i<10;i++)
  {
    v[i]=i;
  }
  return n=10;
}
#if 0
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
#endif
