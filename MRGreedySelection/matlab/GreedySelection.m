function [S, W]= GreedySelection(target, source, k)
% Select k cols of source that best represent the rows of target

target = target';
source = source';

%the algorithm works row-wise
G = source*target';   

[n, p] = size(G);

f = sum(G.^2, 2);
g = sum(source.^2, 2);
W = zeros(n, k);
V = zeros(p, k);
S = zeros(k, 1);
for t=1:k
    % Calculate score function
    score = f./g;
    score(S(1:t-1)) = 0;        % To exclude previously selected features
    score(isnan(score)) = 0;    % To exclude 0/0 entries
    score(isinf(score)) = 0;    % To exclude small-value/0 entries
    
    [~, l] = max(score);
    
    if nnz(score)==0
        W = W(:, 1:t-1);
        S = S(1:t-1, :);
        break;
    end
    if t == 1
        delta = source*source(l, :)';
        gamma = G(l, :)';
    else
        delta = source*source(l, :)' - W * W(l, :)';
        gamma = G(l, :)' - V * W(l, :)';
    end
    
    alpha_sqrt = sqrt(delta(l));
    w = delta./alpha_sqrt;
    v = gamma./alpha_sqrt;
    
    if t == 1
        r2 = 0;
    else
        r2 = W * (v'*V)';
    end
    
    if sum(delta.^2) < 1e-40 || alpha_sqrt < 1e-40      % t > rank(K)
        break;
    end
    
    r1 = G*v;
    r3 = w.*w;
            
    f = f - 2*(w .* (r1 - r2)) + (norm(v)).^2 * r3;
    g = g - r3;
    
    f(l) = 0; f(f<1e-10) = 0;
    g(l) = 0; g(g<1e-10) = 0;    
    W(:, t) = w;
    V(:, t) = v;
    S(t) = l;
end

S = S(S>0);
