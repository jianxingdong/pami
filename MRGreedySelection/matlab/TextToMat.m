function [ output_args ] = TextToMat(TextDir, MatDir)
partsList = dir(TextDir);
numParts = size(partsList,1); 
for i = 3: numParts
    A = importdata(strcat(TextDir,partsList(i).name),',');
    save(strcat(MatDir,num2str(i-2),'.mat'), 'A');
end
end

