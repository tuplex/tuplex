echo "building shared libs"
g++ -shared -fPIC -O3 -o count_m_full_int.so count_m_full.cc 
g++ -shared -fPIC -O3 -o count_m_full_string.so count_m_full.cc -DSTR
g++ -shared -fPIC -O3 -o count_um_full_int.so count_um_full.cc 
g++ -shared -fPIC -O3 -o count_um_full_string.so count_um_full.cc -DSTR
