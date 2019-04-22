echo "masstree configure..."
cd ../third-party/masstree-beta
./bootstrap.sh
./configure --disable-superpage CXX='g++ -std=c++11'
