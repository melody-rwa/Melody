// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

interface ILPManager {
    // Add liquidity (BNB + token). Returns lpTokenId and optionally amounts used.
    struct MintResult {
        uint256 lpTokenId;
        uint256 amountToken;
        uint256 amountBNB;
    }
    function addLiquidity(address token, uint256 tokenAmount, uint256 minToken, uint256 minBNB) external payable returns (MintResult memory);
    // collect fees from LP position - implementation dependent
    function collectFees(uint256 lpTokenId) external returns (uint256 collectedToken, uint256 collectedBNB);
}
