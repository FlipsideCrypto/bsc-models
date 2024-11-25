{{ config(
    materialized = 'table',
    tags = ['curated']
) }}

WITH vtoken_pulls AS (

    SELECT
        *
    FROM
        {{ ref('silver__contracts') }} C
    WHERE
        contract_address IN (
            '0x26da28954763b92139ed49283625cecaf52c6f94',
            '0xc4ef4229fec74ccfe17b2bdef7715fac740ba0ba',
            '0x9a0af7fdb2065ce470d72664de73cae409da28ec',
            '0x5f0388ebc2b94fa8e123f404b79ccf5f40b29176',
            '0x972207a639cc1b374b893cc33fa251b55ceb7c07',
            '0xa07c5b74c9b40447a954e1466938b865b6bbea36',
            '0x882c173bc7ff3b7786ca16dfed3dfffb9ee7847b',
            '0x95c78222b3d6e262426483d42cfa53685a67ab9d',
            '0x86ac3974e2bd0d60825230fa6f355ff11409df5c',
            '0x334b3ecb4dca3593bccc3c7ebd1a1c1d1780fbf1',
            '0xec3422ef92b2fb59e84c8b02ba73f1fe84ed8d71',
            '0x1610bc33319e9398de5f57b33a5b184c806ad217',
            '0xf508fcd89b8bd15579dc79a6827cb4686a3592c8',
            '0xf91d58b5ae142dacc749f58a49fcbac340cb0343',
            '0x650b940a1033b8a1b1873f78730fcfc73ec11f1f',
            '0x57a5297f2cb2c0aac9d554660acd6d385ab50c6b',
            '0xb91a659e88b51474767cd97ef3196a3e7cedd2c8',
            '0x5c9476fcd6a4f9a3654139721c949c2233bbbbc8',
            '0x2ff3d0f6990a40261c66e1ff2017acbc282eb6d0',
            '0xc5d3466aa484b040ee977073fcf337f2c00071c1',
            '0x61edcfe8dd6ba3c891cb9bec2dc7657b3b422e93',
            '0xbf762cd5991ca1dcddac9ae5c638f5b5dc3bee6e',
            '0x08ceb3f4a7ed3500ca0982bcd0fc7816688084c3',
            '0x27ff564707786720c71a2e5c1490a63266683612',
            '0xeca88125a5adbe82614ffc12d0db554e2e2867c8',
            '0xfd5840cd36d94d7229439859c0112a4185bc0255',
            '0x78366446547d062f45b4c0f320cdaa6d710d87bb',
            '0x6cfdec747f37daf3b87a35a1d9c8ad3063a1a8a0',
            '0xb248a295732e0225acd3337607cc01068e3b9c10',
            '0x151b1e2635a717bcdc836ecd6fbb62b674fe3e1d'
        )
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
underlying_add AS (
    SELECT
        *,
        CASE
            WHEN contract_address = '0x26da28954763b92139ed49283625cecaf52c6f94' THEN '0xfb6115445bff7b52feb98650c87f44907e58f802'
            WHEN contract_address = '0x9a0af7fdb2065ce470d72664de73cae409da28ec' THEN '0x3ee2200efb3400fabb9aacf31297cbdd1d435d47'
            WHEN contract_address = '0x5f0388ebc2b94fa8e123f404b79ccf5f40b29176' THEN '0x8ff795a6f4d97e7887c79bea79aba5cc76444adf'
            WHEN contract_address = '0x972207a639cc1b374b893cc33fa251b55ceb7c07' THEN '0x250632378e573c6be1ac2f97fcdf00515d0aa91b'
            WHEN contract_address = '0xa07c5b74c9b40447a954e1466938b865b6bbea36' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            WHEN contract_address = '0x882c173bc7ff3b7786ca16dfed3dfffb9ee7847b' THEN '0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c'
            WHEN contract_address = '0x95c78222b3d6e262426483d42cfa53685a67ab9d' THEN '0xe9e7cea3dedca5984780bafc599bd69add087d56'
            WHEN contract_address = '0x86ac3974e2bd0d60825230fa6f355ff11409df5c' THEN '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'
            WHEN contract_address = '0x334b3ecb4dca3593bccc3c7ebd1a1c1d1780fbf1' THEN '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3'
            WHEN contract_address = '0xec3422ef92b2fb59e84c8b02ba73f1fe84ed8d71' THEN '0xba2ae424d960c26247dd6c32edc70b295c744c43'
            WHEN contract_address = '0x1610bc33319e9398de5f57b33a5b184c806ad217' THEN '0x7083609fce4d1d8dc0c979aab8c869ea2c873402'
            WHEN contract_address = '0xf508fcd89b8bd15579dc79a6827cb4686a3592c8' THEN '0x2170ed0880ac9a755fd29b2688956bd959f933f8'
            WHEN contract_address = '0xf91d58b5ae142dacc749f58a49fcbac340cb0343' THEN '0x0d8ce2a99bb6e3b7db580ed848240e4a0f9ae153'
            WHEN contract_address = '0x650b940a1033b8a1b1873f78730fcfc73ec11f1f' THEN '0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd'
            WHEN contract_address = '0x57a5297f2cb2c0aac9d554660acd6d385ab50c6b' THEN '0x4338665cbb7b2485a8855a139b75d5e34ab0db94'
            WHEN contract_address = '0xb91a659e88b51474767cd97ef3196a3e7cedd2c8' THEN '0x156ab3346823b651294766e23e6cf87254d68962'
            WHEN contract_address = '0x5c9476fcd6a4f9a3654139721c949c2233bbbbc8' THEN '0xcc42724c6683b7e57334c4e856f4c9965ed682bd'
            WHEN contract_address = '0x2ff3d0f6990a40261c66e1ff2017acbc282eb6d0' THEN '0x47bead2563dcbf3bf2c9407fea4dc236faba485a'
            WHEN contract_address = '0xc5d3466aa484b040ee977073fcf337f2c00071c1' THEN '0xce7de646e7208a4ef112cb6ed5038fa6cc6b12e3'
            WHEN contract_address = '0x61edcfe8dd6ba3c891cb9bec2dc7657b3b422e93' THEN '0x85eac5ac2f758618dfa09bdbe0cf174e7d574d5b'
            WHEN contract_address = '0xbf762cd5991ca1dcddac9ae5c638f5b5dc3bee6e' THEN '0x40af3827f39d0eacbf4a168f8d4ee67c121d11c9'
            WHEN contract_address = '0x08ceb3f4a7ed3500ca0982bcd0fc7816688084c3' THEN '0x14016e85a25aeb13065688cafb43044c2ef86784'
            WHEN contract_address = '0x27ff564707786720c71a2e5c1490a63266683612' THEN '0xbf5140a22578168fd562dccf235e5d43a02ce9b1'
            WHEN contract_address = '0xeca88125a5adbe82614ffc12d0db554e2e2867c8' THEN '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'
            WHEN contract_address = '0xfd5840cd36d94d7229439859c0112a4185bc0255' THEN '0x55d398326f99059ff775485246999027b3197955'
            WHEN contract_address = '0x78366446547d062f45b4c0f320cdaa6d710d87bb' THEN '0x3d4350cd54aef9f9b2c29435e0fa809957b3f30a'
            WHEN contract_address = '0x6cfdec747f37daf3b87a35a1d9c8ad3063a1a8a0' THEN '0xa2e3356610840701bdf5611a53974510ae27e2e1'
            WHEN contract_address = '0xb248a295732e0225acd3337607cc01068e3b9c10' THEN '0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe'
            WHEN contract_address = '0x151b1e2635a717bcdc836ecd6fbb62b674fe3e1d' THEN '0xcf6bb5389c92bdda8a3747ddb454cb7a64626c63'
            WHEN contract_address = '0xc4ef4229fec74ccfe17b2bdef7715fac740ba0ba' THEN '0xc5f0f7b66764f6ec8c8dff7ba683102295e16409'
            ELSE NULL
        END AS underlying_address
    FROM
        vtoken_pulls
)
SELECT
    l.contract_address as itoken_address,
    l.token_name as itoken_name,
    l.token_symbol as itoken_symbol,
    l.token_decimals as itoken_decimals,
    l.underlying_address AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_decimals AS underlying_decimals,
    C.token_symbol AS underlying_symbol
FROM
    underlying_add l
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON C.contract_address = l.underlying_address
