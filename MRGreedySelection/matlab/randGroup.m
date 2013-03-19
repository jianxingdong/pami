function [ target ] = randGroup( source, numGroups )
    n = size(source,2);
   % Random hard cluster-membership function  
    M = ceil(rand(n, 1)*numGroups);
    R = zeros(numGroups, n);
    R([1:numGroups:numel(R)]' + M - 1) = 1;
    clear M;

    vSize = sum(R, 2);
    I = vSize>0;

    % Exculde empty groups
    R = R(I, :);           
    target = source*R';
    clear R;
end

