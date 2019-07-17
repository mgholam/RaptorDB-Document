md dist
md public
copy src\index.html public\
copy src\index.html dist\
copy src\global.css public\
copy src\global.css dist\

npm run build
