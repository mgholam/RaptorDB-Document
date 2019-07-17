"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\msbuild.exe" /p:buildmode=Debug /t:rebuild raptordbtest.sln
dotnet build raptordbcore.sln
.nuget\NuGet.exe pack raptordb_doc.nuspec